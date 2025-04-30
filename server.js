process.env.TZ = 'Asia/Kolkata';
const express = require('express');
const Redis = require('ioredis');
const cors = require('cors');
const mqtt = require('mqtt');
const cluster = require('cluster');
const os = require('os');
const axios = require('axios');

const numCPUs = os.cpus().length;
const EMQX_HOST = 'MQTT-chat-7c6cdb28f96eeaf6.elb.ap-south-1.amazonaws.com';

if (cluster.isMaster) {
  console.log(`Master process running with PID: ${process.pid}`);
  for (let i = 0; i < numCPUs; i++) cluster.fork();
  cluster.on('exit', (worker) => {
    console.log(`Worker ${worker.process.pid} died. Restarting...`);
    cluster.fork();
  });
} else {
  const app = express();
  app.use(express.json());
  app.use(cors());

  const redis = new Redis({
    host: 'nexttoppers-serverless-pa60px.serverless.aps1.cache.amazonaws.com',
    port: 6379,
    tls: {}
  });

  redis.ping()
    .then(() => console.log('✅ Redis connected'))
    .catch(err => {
      console.error('❌ Redis connection failed:', err);
      process.exit(1);
    });

  const generateSixDigitNumber = () => Math.floor(100000 + Math.random() * 900000);

  app.get('/poll/health', (req, res) => res.status(200).send('OK'));

  app.post('/managePoll', async (req, res) => {
    const { type, data, course_id, id: userId, setting_node, video_id } = req.body;
    if (!type || !video_id) {
      return res.status(400).json({ error: 'Invalid payload structure' });
    }

    try {
      switch (type) {
        case 'CREATE_POLL': {
          const pollId = parseInt(video_id + generateSixDigitNumber());
          const duration = parseInt(data.validity) || 60;
          const now = Math.floor(Date.now() / 1000);
          const validTill = now + duration;

          const fullPayload = {
            type: 'poll',
            poll_id: pollId,
            date: now,
            is_active: '1',
            name: data.name,
            profile_picture: '',
            pin: '0',
            user_id: userId,
            message: {
              question: data.question,
              option_1: data.option_1,
              option_2: data.option_2,
              option_3: data.option_3,
              option_4: data.option_4,
              answer: data.answer,
              created: now,
              delay: data.delay,
              validity: data.validity,
              valid_till: validTill,
              disable_result: 0,
              video_id: video_id,
              status: '1',
              attempt_1: 0,
              attempt_2: 0,
              attempt_3: 0,
              attempt_4: 0,
              id: String(pollId),
              firebase_key: String(pollId),
              poll_key: String(pollId),
              poll_id: String(pollId)
            },
            course_id: course_id,
            video_id: video_id,
            id: String(pollId)
          };

          // Store metadata
          const metaKey = `Poll:${pollId}:meta`;
          await redis.hmset(metaKey, fullPayload.message);
          await redis.expire(metaKey, duration + 300);

          // Initialize vote counts
          const votesKey = `Poll:${pollId}:votes`;
          await redis.hmset(votesKey, { 1: 0, 2: 0, 3: 0, 4: 0 });
          await redis.expire(votesKey, duration + 300);

          // Track polls per video
          const videoSetKey = `VideoPolls:${video_id}`;
          await redis.sadd(videoSetKey, pollId);
          await redis.expire(videoSetKey, duration + 300);

          // Publish via MQTT
          const mqttClient = mqtt.connect(`mqtt://${EMQX_HOST}:1883`, {
            clientId: 'PollService-' + Math.random().toString(16).substr(2, 8),
            clean: true,
            reconnectPeriod: 1000
          });
          mqttClient.on('connect', () => {
            mqttClient.publish(setting_node, JSON.stringify(fullPayload), { qos: 2, retain: false }, (err) => {
              if (err) console.error('❌ Error publishing:', err);
              console.log('Published:', JSON.stringify(fullPayload));
              mqttClient.end();
            });
          });

          // Schedule final leaderboard
          setTimeout(() => finalizePoll(pollId, fullPayload.message), duration * 1000);

          return_data(res, { type, message: 'Poll Created', data: fullPayload, status_code: 10011 });
          break;
        }

        case 'UPDATE_POLL': {
          const pollId = req.body.poll_id;
          const userIdUpdate = req.body.user_id;
          const selectedOption = req.body.attempted;
          const responseTime = req.body.timeleft;
          const name = req.body.name;

          const meta = await redis.hgetall(`Poll:${pollId}:meta`);
          if (!meta || Object.keys(meta).length === 0) {
            return return_data(res, { type, message: 'Poll not found', data: [] });
          }

          const isCorrect = selectedOption === meta.answer;
          const answeredSet = `Poll:${pollId}:users_answered`;
          const added = await redis.sadd(answeredSet, userIdUpdate);
          if (added === 0) {
            return return_data(res, { type, message: 'Poll already answered', data: [] });
          }

          const pipeline = redis.pipeline();
          // Increment vote count
          pipeline.hincrby(`Poll:${pollId}:votes`, selectedOption, 1);

          // Record user response
          const userKey = `Poll:${pollId}:user:${userIdUpdate}`;
          const record = {
            selected_option: selectedOption,
            is_correct: isCorrect ? 1 : 0,
            response_time: responseTime,
            user_id: userIdUpdate,
            name: name
          };
          pipeline.hmset(userKey, record);
          pipeline.expire(userKey, 14400);

          // Add to leaderboard sorted set
          if (isCorrect) {
            pipeline.zadd(`Poll:${pollId}:leaderboard`, responseTime, JSON.stringify(record));
          }
          await pipeline.exec();

          return_data(res, { type, message: 'Poll Attempted', data: req.body, status_code: 10012 });
          break;
        }

        case 'GET_POLL': {
          const pollId = req.body.poll_id;
          const userIdGet = req.body.user_id;

          const pipeline = redis.pipeline();
          pipeline.hgetall(`Poll:${pollId}:meta`);
          pipeline.hgetall(`Poll:${pollId}:user:${userIdGet}`);
          const results = await pipeline.exec();
          const pollData = results[0][1] || {};
          const userResp = results[1][1] || {};

          if (!pollData || Object.keys(pollData).length === 0) {
            return return_data(res, { type, message: 'Poll not found', data: [] });
          }
          pollData.my_answer = userResp.selected_option || 0;

          return_data(res, { type, message: 'Poll Details', data: { user_id: userIdGet, poll_id: pollId, type: 'poll', message: pollData }, status_code: 10011 });
          break;
        }

        case 'GET_LEADERBOARD': {
          const pollId = req.body.poll_id;
          let leaderboardData = await redis.get(`Poll:${pollId}:final_leaderboard`);
          if (!leaderboardData) {
            const pollMeta = await redis.hgetall(`Poll:${pollId}:meta`);
            await finalizePoll(pollId, pollMeta);
            leaderboardData = await redis.get(`Poll:${pollId}:final_leaderboard`);
          }
          return_data(res, { type, message: 'Poll leaderboard generated', data: leaderboardData, status_code: 10011 });
          break;
        }

        case 'GET_LEADERBOARD_VIDEOWISE': {
          const videoIdParam = req.body.video_id;
          const setKey = `VideoPolls:${videoIdParam}`;
          const pollIds = await redis.smembers(setKey);
          if (!pollIds.length) {
            return return_data(res, { type, message: 'No polls found for video', data: [] });
          }
          let all = [];
          for (const pid of pollIds) {
            const raw = await redis.get(`Poll:${pid}:final_leaderboard`);
            const arr = raw ? JSON.parse(raw) : [];
            all.push(...arr);
          }
          all.sort((a, b) => a.timetaken - b.timetaken);
          return_data(res, { type, message: 'Poll leaderboard generated', data: all, status_code: 10011 });
          break;
        }

        default:
          return_data(res, { type, message: 'Unknown type', data: [] });
      }
    } catch (err) {
      console.error('❌ Error handling managePoll:', err);
      res.status(500).send('Internal error');
    }
  });

  async function finalizePoll(pollId, fullPayload) {
    try {
      const topRaw = await redis.zrange(`Poll:${pollId}:leaderboard`, 0, 9, 'WITHSCORES');
      const leaderboard = [];
      let rankCount = 0;
      for (let i = 0; i < topRaw.length; i += 2) {
        const entry = JSON.parse(topRaw[i]);
        leaderboard.push({
          user_id: entry.user_id,
          name: entry.name,
          answer: entry.answer,
          rank: ++rankCount,
          timetaken: parseFloat(topRaw[i + 1])
        });
      }
      await redis.set(`Poll:${pollId}:final_leaderboard`, JSON.stringify(leaderboard), 'EX', 14400);
      fullPayload.poll_key = pollId;
      await axios.post('https://admin.videocrypt.in/welcome/create_poll_from_socket', fullPayload).catch(err => console.log(err));
      console.log(`✅ Poll ${pollId} finalized.`);
    } catch (err) {
      console.error(`❌ Error finalizing poll ${pollId}:`, err);
    }
  }

  function return_data(res, dataObj) {
    return res.status(200).json(dataObj);
  }

  const PORT = process.env.PORT || 8080;
  app.listen(PORT, () => console.log(`Worker ${process.pid} running on port ${PORT}`));
}
