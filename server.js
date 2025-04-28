// server.js
process.env.TZ = 'Asia/Kolkata';
const express = require("express");
const Redis = require("ioredis");
const cors = require("cors");
const mqtt = require("mqtt");
const cluster = require("cluster");
const os = require("os");

const numCPUs = os.cpus().length;
const EMQX_HOST   = process.env.EMQX_HOST   || "MQTT-chat-7c6cdb28f96eeaf6.elb.ap-south-1.amazonaws.com";
const REDIS_HOST  = process.env.REDIS_HOST  || "nexttoppers-serverless-pa60px.serverless.aps1.cache.amazonaws.com";
const REDIS_PORT  = parseInt(process.env.REDIS_PORT, 10) || 6379;
const REDIS_TLS   = process.env.REDIS_TLS === "true";

if (cluster.isMaster) {
  console.log(`Master PID ${process.pid}`);
  for (let i = 0; i < numCPUs; i++) cluster.fork();
  cluster.on("exit", w => {
    console.log(`Worker ${w.process.pid} died—restarting`);
    cluster.fork();
  });
} else {
  const app = express();
  app.use(express.json());
  app.use(cors());

  // — Redis setup
  const redis = new Redis({
    host: REDIS_HOST,
    port: REDIS_PORT,
    tls: REDIS_TLS ? {} : undefined
  });
  redis.ping()
    .then(() => console.log("✅ Redis connected"))
    .catch(err => { console.error("❌ Redis error:", err); process.exit(1); });

  // — MQTT setup
  let isMqttConnected = false;
  const mqttClient = mqtt.connect(`mqtt://${EMQX_HOST}:1883`, {
    clientId: `PollService-${Math.random().toString(16).slice(2)}`,
    clean: true,
    reconnectPeriod: 1000
  });
  mqttClient.on("connect", () =>  { console.log("✅ MQTT connected");   isMqttConnected = true;  });
  mqttClient.on("error",   e =>  { console.error("❌ MQTT error:", e); isMqttConnected = false; });
  mqttClient.on("close",   () => { console.warn ("⚠️ MQTT closed");   isMqttConnected = false; });
  mqttClient.on("offline", () => { console.warn ("⚠️ MQTT offline");  isMqttConnected = false; });

  // — Single entrypoint
  app.post("/managePoll", async (req, res) => {
    // 1) unpack & default
    const { type, data = {}, ...rest } = req.body;
    // 2) merge nested + top-level + enforce type
    const payload = { ...data, ...rest, type };

    if (!payload.type) {
      return res.status(400).json({ type: null, message: "Missing type", data: {} });
    }

    try {
      switch (payload.type) {
        // ───────────────────────── CREATE_POLL ─────────────────────────
        case "CREATE_POLL": {
          // destruct with defaults
          const {
            id,
            video_id,
            setting_node,
            question   = payload.name || "Poll Title",
            option_1, option_2, option_3, option_4, option_5, option_6,
            answer,
            validity   = "60",
            delay      = "1"
          } = payload;

          const poll_id    = id || `${video_id}${Date.now()}`;
          const createdTS  = Math.floor(Date.now() / 1000);
          const valid_till = createdTS + parseInt(validity, 10);
          const ttlSeconds = parseInt(validity, 10) + 60; // buffer

          // — store metadata
          await redis.hmset(`Poll:${poll_id}:meta`, {
            question,
            option_1, option_2, option_3, option_4, option_5, option_6,
            correct_option: String(answer),
            start_time: String(createdTS),
            validity: String(validity)
          });
          await redis.expire(`Poll:${poll_id}:meta`, ttlSeconds);

          // — init votes
          [option_1,option_2,option_3,option_4,option_5,option_6]
            .filter(o => o != null)
            .forEach((_, i) => redis.hset(`Poll:${poll_id}:votes`, String(i+1), "0"));
          await redis.expire(`Poll:${poll_id}:votes`, ttlSeconds);

          // — publish MQTT
          if (isMqttConnected) {
            const mqttPayload = {
              poll_id,
              type: "poll",
              message: { question, option_1, option_2, option_3, option_4, option_5, option_6, answer,
                         created: createdTS, delay: parseInt(delay,10), validity: parseInt(validity,10), valid_till }
            };
            mqttClient.publish(setting_node, JSON.stringify(mqttPayload),
              { qos: 1, retain: true },
              err => err
                ? console.error("❌ MQTT publish error:", err)
                : console.log("📡 Poll published:", mqttPayload)
            );
          } else {
            console.error("❌ MQTT not connected—skipping publish");
          }

          // — schedule finalize
          setTimeout(() => finalizePoll(poll_id), (parseInt(delay,10) + parseInt(validity,10)) * 1000);

          return res.json({
            type:    "CREATE_POLL",
            message: "Poll Created",
            data:    { poll_id }
          });
        }

        // ───────────────────────── UPDATE_POLL ─────────────────────────
        case "UPDATE_POLL": {
          const { poll_id, user_id, attempted, timeleft } = payload;
          if (!poll_id)  return res.status(400).json({ type, message:"Missing poll_id", data:{} });
          if (!user_id)  return res.status(400).json({ type, message:"Missing user_id", data:{} });

          // fetch meta
          const meta = await redis.hgetall(`Poll:${poll_id}:meta`);
          if (!meta.correct_option) {
            return res.status(404).json({ type, message:"Poll not found", data:{} });
          }

          const answeredSet = `Poll:${poll_id}:users_answered`;
          const added       = await redis.sadd(answeredSet, String(user_id));
          if (added === 0) {
            return res.json({ type, message:"Already answered", data:{} });
          }

          // record vote
          const isCorrect = String(attempted) === meta.correct_option;
          const pipeline  = redis.pipeline();
          pipeline.hincrby(`Poll:${poll_id}:votes`, String(attempted), 1);
          pipeline.hmset(`Poll:${poll_id}:user:${user_id}`, {
            selected_option: String(attempted),
            is_correct:      String(isCorrect),
            response_time:   String(timeleft)
          });
          if (isCorrect) {
            pipeline.zadd(`Poll:${poll_id}:leaderboard`, timeleft, String(user_id));
          }
          pipeline.expire(`Poll:${poll_id}:user:${user_id}`, parseInt(meta.validity,10) + 60);
          await pipeline.exec();

          return res.json({
            type:    "UPDATE_POLL",
            message: "Poll Attempt Recorded",
            data:    {}
          });
        }

        // ───────────────────────── GET_POLL ─────────────────────────
        case "GET_POLL": {
          const { poll_id, user_id } = payload;
          if (!poll_id) return res.status(400).json({ type, message:"Missing poll_id", data:{} });

          const meta = await redis.hgetall(`Poll:${poll_id}:meta`);
          if (!meta.question) {
            return res.status(404).json({ type, message:"Poll not found", data:{} });
          }

          // build response message
          const msg = {
            question: meta.question,
            validity: parseInt(meta.validity,10)
          };
          [1,2,3,4,5,6].forEach(i => {
            if (meta[`option_${i}`]) msg[`option_${i}`] = meta[`option_${i}`];
          });

          // fetch user answer
          let my_answer = null;
          if (user_id) {
            const uv = await redis.hgetall(`Poll:${poll_id}:user:${user_id}`);
            if (uv.selected_option) my_answer = uv.selected_option;
          }

          return res.json({
            type:    "GET_POLL",
            message: "Poll Retrieved",
            data:    { message: msg, my_answer }
          });
        }

        // ───────────────────────── GET_RESULT ─────────────────────────
        case "GET_RESULT": {
          const { poll_id } = payload;
          if (!poll_id) return res.status(400).json({ type, message:"Missing poll_id", data:{} });

          const votes = await redis.hgetall(`Poll:${poll_id}:votes`);
          return res.json({
            type:    "GET_RESULT",
            message: "Poll Results",
            data:    { votes }
          });
        }

        // ─────────── GET_LEADERBOARD & GET_LEADERBOARD_VIDEOWISE ───────────
        case "GET_LEADERBOARD":
        case "GET_LEADERBOARD_VIDEOWISE": {
          const { poll_id } = payload;
          if (!poll_id) return res.status(400).json({ type, message:"Missing poll_id", data:{} });

          const raw = await redis.get(`Poll:${poll_id}:final_leaderboard`);
          if (!raw) {
            return res.status(404).json({ type, message:"Leaderboard not ready", data:{} });
          }

          return res.json({
            type:    payload.type,
            message: "Poll Leaderboard",
            data:    JSON.parse(raw)
          });
        }

        // ───────────────────────── UNKNOWN TYPE ─────────────────────────
        default:
          return res.status(400).json({
            type:    payload.type,
            message: "Unknown type",
            data:    {}
          });
      }
    } catch (err) {
      console.error("Handler error:", err);
      return res.status(500).json({
        type:    payload.type,
        message: "Internal error",
        data:    {}
      });
    }
  });

  // — finalizePoll (runs after delay+validity)
  async function finalizePoll(pollId) {
    try {
      const top = await redis.zrange(`Poll:${pollId}:leaderboard`, 0, 9, "WITHSCORES");
      const lb  = [];
      for (let i = 0; i < top.length; i += 2) {
        lb.push({ user_id: top[i], time: parseFloat(top[i+1]) });
      }
      await redis.set(
        `Poll:${pollId}:final_leaderboard`,
        JSON.stringify(lb),
        "EX", 3600
      );
      console.log(`✅ Poll ${pollId} finalized`);
    } catch (e) {
      console.error("Finalize error:", e);
    }
  }

  // — start server
  const PORT = process.env.PORT || 8080;
  app.listen(PORT, () => {
    console.log(`Worker ${process.pid} listening on ${PORT}`);
  });
}
