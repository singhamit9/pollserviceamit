const express = require("express");
const Redis = require("ioredis");
const cors = require("cors");
const mqtt = require("mqtt");
const cluster = require("cluster");
const os = require("os");

const numCPUs = os.cpus().length;
const EMQX_HOST = process.env.EMQX_HOST || "MQTT-chat-7c6cdb28f96eeaf6.elb.ap-south-1.amazonaws.com";

if (cluster.isMaster) {
  console.log(`Master process running with PID: ${process.pid}`);
  for (let i = 0; i < numCPUs; i++) cluster.fork();
  cluster.on("exit", (worker) => {
    console.log(`Worker ${worker.process.pid} died. Restarting...`);
    cluster.fork();
  });
} else {
  const app = express();
  app.use(express.json());
  app.use(cors());

  app.get("/poll/health", (req, res) => res.status(200).send("OK"));

  const redis = new Redis({
    host: process.env.REDIS_HOST || "nexttoppers-serverless-pa60px.serverless.aps1.cache.amazonaws.com",
    port: parseInt(process.env.REDIS_PORT, 10) || 6379,
    tls: {},
  });

  redis.ping()
    .then(() => console.log("✅ Redis connected"))
    .catch((err) => {
      console.error("❌ Redis connection failed:", err);
      process.exit(1);
    });

  let isMqttConnected = false;
  const mqttClient = mqtt.connect(`mqtt://${EMQX_HOST}:1883`, {
    clientId: "PollService-" + Math.random().toString(16).substr(2, 8),
    clean: true,
    reconnectPeriod: 1000,
  });

  mqttClient.on("connect", () => {
    console.log("✅ Connected to MQTT broker");
    isMqttConnected = true;
  });
  mqttClient.on("error", (err) => {
    console.error("❌ MQTT error:", err);
    isMqttConnected = false;
  });
  mqttClient.on("close", () => (isMqttConnected = false));
  mqttClient.on("offline", () => (isMqttConnected = false));

  app.post("/managePoll", async (req, res) => {
    const { type, data } = req.body;
    if (!type) return res.status(400).json({ type: null, message: "Invalid payload", data: {} });

    try {
      switch (type) {
        case "CREATE_POLL": {
          const setting_node = req.body.setting_node;
          const course_id = req.body.course_id || "";
          const video_id = req.body.video_id || "";
          const pollId = req.body.id || video_id + Date.now();
          const createdTime = Math.floor(Date.now() / 1000);
          const validity = parseInt(data.validity || "60", 10);

          await redis.hmset(`Poll:${pollId}:meta`, {
            question: data.question,
            options: JSON.stringify({
              option_1: data.option_1 || "",
              option_2: data.option_2 || "",
              option_3: data.option_3 || "",
              option_4: data.option_4 || "",
            }),
            correct_option: data.answer,
            start_time: createdTime,
            duration: validity,
          });
          redis.expire(`Poll:${pollId}:meta`, validity + 60);

          const payload = {
            poll_id: pollId,
            course_id,
            video_id,
            type: "poll",
            date: createdTime,
            is_active: "1",
            name: data.question,
            platform: req.body.platform,
            message: {
              question: data.question,
              option_1: data.option_1,
              option_2: data.option_2,
              option_3: data.option_3,
              option_4: data.option_4,
              answer: data.answer,
              validity: validity,
            },
            firebase_key: pollId,
            poll_key: pollId,
            id: pollId,
          };

          if (!isMqttConnected) {
            return res.status(500).json({ type, message: "MQTT not connected", data: {} });
          }
          mqttClient.publish(setting_node, JSON.stringify(payload), { qos: 1, retain: true });

          setTimeout(() => finalizePoll(pollId), validity * 1000);
          return res.json({ type, message: "Poll Created", data: payload });
        }

        case "UPDATE_POLL": {
          const pollId = req.body.poll_id || data.poll_id;
          const userId = req.body.user_id || data.user_id;
          const selectedOption = req.body.attempted || data.attempted;
          const responseTime = req.body.timeleft || data.timeleft;

          const meta = await redis.hgetall(`Poll:${pollId}:meta`);
          if (!meta.correct_option) {
            return res.status(404).json({ type, message: "Poll not found", data: {} });
          }

          const answeredSet = `Poll:${pollId}:users_answered`;
          const added = await redis.sadd(answeredSet, userId);
          if (added === 0) {
            return res.json({ type, message: "Already answered", data: {} });
          }

          const pipeline = redis.pipeline();
          pipeline.hincrby(`Poll:${pollId}:votes`, selectedOption, 1);
          pipeline.hmset(`Poll:${pollId}:user:${userId}`, {
            selected_option: selectedOption,
            is_correct: selectedOption === meta.correct_option,
            response_time: responseTime,
          });
          pipeline.zadd(`Poll:${pollId}:leaderboard`, responseTime, userId);
          pipeline.expire(`Poll:${pollId}:user:${userId}`, 3600);
          await pipeline.exec();

          return res.json({ type, message: "Poll Attempted", data: { poll_id: pollId, user_id: userId, selected_option: selectedOption, response_time: responseTime } });
        }

        case "GET_POLL": {
          const pollId = req.body.poll_id || data.poll_id;
          const userId = req.body.user_id || data.user_id;
          const meta = await redis.hgetall(`Poll:${pollId}:meta`);
          if (!meta || !meta.options) {
            return res.status(404).json({ type, message: "Poll not found", data: {} });
          }

          let opts;
          try { opts = JSON.parse(meta.options); } catch { opts = {}; }

          let myAnswer = null;
          if (userId) {
            const userVote = await redis.hgetall(`Poll:${pollId}:user:${userId}`);
            myAnswer = userVote.selected_option || null;
          }

          return res.json({ type, message: "Poll List", data: { question: meta.question, options: opts, my_answer: myAnswer } });
        }

        case "GET_RESULT": {
          const pollId = req.body.poll_id || data.poll_id;
          const votes = await redis.hgetall(`Poll:${pollId}:votes`);
          return res.json({ type, message: "Poll Results", data: votes });
        }

        case "GET_LEADERBOARD":
        case "GET_LEADERBOARD_VIDEOWISE": {
          const pollId = req.body.poll_id || data.poll_id;
          const boardRaw = await redis.get(`Poll:${pollId}:final_leaderboard`);
          if (!boardRaw) {
            return res.status(404).json({ type, message: "Leaderboard not ready", data: [] });
          }
          const board = JSON.parse(boardRaw);
          const msg = type === "GET_LEADERBOARD_VIDEOWISE" ? "Complete Video Leaderboard" : "Poll LeaderBoard";
          return res.json({ type, message: msg, data: board });
        }

        default:
          return res.status(400).json({ type, message: "Unknown type", data: {} });
      }
    } catch (err) {
      console.error(err);
      return res.status(500).json({ type, message: "Internal server error", data: {} });
    }
  });

  async function finalizePoll(pollId) {
    try {
      const top = await redis.zrange(`Poll:${pollId}:leaderboard`, 0, 9, "WITHSCORES");
      const leaderboard = [];
      for (let i = 0; i < top.length; i += 2) {
        leaderboard.push({ user_id: top[i], time: parseFloat(top[i+1]) });
      }
      await redis.set(`Poll:${pollId}:final_leaderboard`, JSON.stringify(leaderboard), "EX", 3600);
      console.log(`Poll ${pollId} finalized.`);
    } catch (e) {
      console.error(`Error finalizing poll ${pollId}:`, e);
    }
  }

  const PORT = process.env.PORT || 8080;
  app.listen(PORT, () => console.log(`Worker ${process.pid} on port ${PORT}`));
}
