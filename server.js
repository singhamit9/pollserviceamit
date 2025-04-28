const express = require('express');
const Redis = require('ioredis');
const cors = require('cors');
const mqtt = require('mqtt');
const cluster = require('cluster');
const os = require('os');

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

  app.get('/poll/health', (req, res) => res.status(200).send('OK'));

  const redis = new Redis({
    host: 'nexttoppers-serverless-pa60px.serverless.aps1.cache.amazonaws.com',
    port: 6379,
    tls: {}
  });

  redis.ping().then(() => console.log('✅ Redis connected')).catch(err => {
    console.error('❌ Redis connection failed:', err);
    process.exit(1);
  });

  let isMqttConnected = false;
  const mqttClient = mqtt.connect(`mqtt://${EMQX_HOST}:1883`, {
    clientId: 'PollService-' + Math.random().toString(16).substr(2, 8),
    clean: true,
    reconnectPeriod: 1000
  });

  mqttClient.on('connect', () => {
    console.log('✅ Connected to MQTT broker (EMQX)');
    isMqttConnected = true;
  });

  mqttClient.on('error', (err) => {
    console.error('❌ MQTT connection error:', err);
    isMqttConnected = false;
  });

  mqttClient.on('close', () => {
    console.warn('⚠️ MQTT connection closed');
    isMqttConnected = false;
  });

  app.post('/managePoll', async (req, res) => {
    const { type, data } = req.body;
    if (!type) return res.status(400).json({ error: 'Invalid payload: missing type' });

    try {
      switch (type) {
        case 'CREATE_POLL': {
          const setting_node = req.body.setting_node;
          const course_id = req.body.course_id || '';
          const video_id = req.body.video_id || '';
          const pollIdCreate = req.body.id || (video_id + '' + Date.now());

          const createdTime = Math.floor(Date.now() / 1000);
          const validity = parseInt(data.validity || '60');
          const expiryTime = validity + 300; // 5 minutes buffer

          const options = {
            question: data.question,
            option_1: data.option_1,
            option_2: data.option_2,
            option_3: data.option_3,
            option_4: data.option_4,
            option_5: data.option_5,
            option_6: data.option_6,
            answer: data.answer,
            created: createdTime,
            delay: parseInt(data.delay) || (createdTime + 1),
            validity: validity,
            valid_till: createdTime + validity,
            disable_result: 0,
            status: '1',
            video_id: video_id,
            attempt_1: 0,
            attempt_2: 0,
            attempt_3: 0,
            attempt_4: 0,
            id: pollIdCreate,
            firebase_key: pollIdCreate,
            poll_key: pollIdCreate,
            poll_id: pollIdCreate
          };

          await redis.hmset(`Poll:${pollIdCreate}:meta`, {
            question: data.question,
            options: JSON.stringify(options),
            correct_option: data.answer,
            start_time: Date.now(),
            duration: validity
          });
          await redis.expire(`Poll:${pollIdCreate}:meta`, expiryTime);

          await redis.sadd(`VideoPolls:${video_id}`, pollIdCreate);
          await redis.expire(`VideoPolls:${video_id}`, expiryTime);

          const payload = {
            poll_id: pollIdCreate,
            type: 'poll',
            date: createdTime,
            is_active: '1',
            name: data.question || req.body.name || 'Poll Title',
            profile_picture: '',
            pin: '0',
            user_id: '',
            platform: req.body.platform,
            course_id: course_id,
            video_id: video_id,
            id: pollIdCreate,
            message: options
          };

          if (!isMqttConnected) {
            console.error('❌ MQTT not connected, cannot publish poll');
            return res.status(500).json({ error: 'MQTT broker not connected' });
          }

          console.log(`📡 Publishing Poll to MQTT:\nTopic = ${setting_node}\nPayload =`, payload);
          mqttClient.publish(setting_node, JSON.stringify(payload), { qos: 1, retain: true }, (err) => {
            if (err) {
              console.error('❌ Error publishing poll to MQTT:', err);
            } else {
              console.log('📡 Poll published successfully via MQTT');
            }
          });

          setTimeout(() => finalizePoll(pollIdCreate), validity * 1000);
          res.json({ success: true });
          break;
        }

        case 'UPDATE_POLL': {
          if (!data || !data.user_id || !data.poll_id) {
            return res.status(400).json({ error: 'Missing required fields for UPDATE_POLL' });
          }

          const pollIdUpdate = data.poll_id;
          const userIdUpdate = data.user_id;
          const selectedOption = data.attempted;
          const responseTime = data.timeleft;

          const expiryTime = 300 + parseInt(data.validity || '60'); // Buffer

          const userKey = `Poll:${pollIdUpdate}:user:${userIdUpdate}`;
          const answeredSet = `Poll:${pollIdUpdate}:users_answered`;
          const pollMeta = await redis.hgetall(`Poll:${pollIdUpdate}:meta`);
          if (!pollMeta.correct_option) return res.status(404).send('Poll not found');

          const isCorrect = selectedOption === pollMeta.correct_option;
          const added = await redis.sadd(answeredSet, userIdUpdate);
          if (added === 0) return res.status(200).send('Already answered');

          const pipeline = redis.pipeline();
          pipeline.hincrby(`Poll:${pollIdUpdate}:votes`, selectedOption, 1);
          pipeline.hmset(userKey, {
            selected_option: selectedOption,
            is_correct: isCorrect,
            response_time: responseTime
          });
          if (isCorrect) pipeline.zadd(`Poll:${pollIdUpdate}:leaderboard`, responseTime, userIdUpdate);
          pipeline.expire(userKey, expiryTime);
          await pipeline.exec();

          res.json({ success: true });
          break;
        }

        case 'GET_POLL': {
          const pollIdGet = req.body.poll_id || data.poll_id;
          const userIdGet = req.body.user_id || data.user_id;
          const pollData = await redis.hgetall(`Poll:${pollIdGet}:meta`);
          if (!pollData) return res.status(404).send('Poll not found');

          if (pollData.options) {
            try {
              pollData.options = JSON.parse(pollData.options);
            } catch (err) {
              console.error('Error parsing options JSON:', err);
              pollData.options = [];
            }
          }

          let myAnswer = null;
          if (userIdGet) {
            const userVote = await redis.hgetall(`Poll:${pollIdGet}:user:${userIdGet}`);
            if (userVote && userVote.selected_option) {
              myAnswer = userVote.selected_option;
            }
          }

          res.json({ poll: pollData, my_answer: myAnswer });
          break;
        }

        case 'GET_RESULT': {
          const pollId = req.body.poll_id || data.poll_id;
          const userId = req.body.user_id || data.user_id;

          if (!pollId || !userId) return res.status(400).json({ error: 'Missing poll_id or user_id' });

          const votes = await redis.hgetall(`Poll:${pollId}:votes`);
          const user = await redis.hgetall(`Poll:${pollId}:user:${userId}`);
          const correctOption = await redis.hget(`Poll:${pollId}:meta`, 'correct_option');

          if (!user || !votes || !correctOption) return res.status(404).json({ error: 'Poll or user data not found' });

          res.json({
            votes,
            user_selection: user.selected_option,
            is_correct: user.is_correct === 'true',
            correct_option: correctOption
          });
          break;
        }

        case 'GET_LEADERBOARD': {
          const pollIdLeader = req.body.poll_id || data.poll_id;
          const leaderboardData = await redis.get(`Poll:${pollIdLeader}:final_leaderboard`);
          if (!leaderboardData) return res.status(404).json({ error: 'Leaderboard not ready' });
          res.json({ leaderboard: JSON.parse(leaderboardData) });
          break;
        }

        case 'GET_LEADERBOARD_VIDEOWISE': {
          const videoId = req.body.video_id || data.video_id;
          if (!videoId) return res.status(400).json({ error: 'Missing video_id' });

          const pollIds = await redis.smembers(`VideoPolls:${videoId}`);
          if (!pollIds || pollIds.length === 0) return res.status(404).json({ error: 'No polls found for video' });

          const allScores = [];

          for (const pollId of pollIds) {
            const scores = await redis.zrange(`Poll:${pollId}:leaderboard`, 0, -1, 'WITHSCORES');
            for (let i = 0; i < scores.length; i += 2) {
              allScores.push({ user_id: scores[i], time: parseFloat(scores[i + 1]) });
            }
          }

          allScores.sort((a, b) => a.time - b.time);
          const top10 = allScores.slice(0, 10);

          res.json({ leaderboard: top10 });
          break;
        }

        default:
          res.status(400).json({ error: 'Unknown type' });
      }
    } catch (err) {
      console.error('❌ Error handling managePoll:', err);
      res.status(500).send('Internal error');
    }
  });

  async function finalizePoll(pollId) {
    try {
      if (!pollId) {
        console.error('❌ finalizePoll called without valid pollId');
        return;
      }
      const top10 = await redis.zrange(`Poll:${pollId}:leaderboard`, 0, 9, 'WITHSCORES');
      const leaderboard = [];
      for (let i = 0; i < top10.length; i += 2) {
        leaderboard.push({ user_id: top10[i], time: parseFloat(top10[i + 1]) });
      }
      await redis.set(`Poll:${pollId}:final_leaderboard`, JSON.stringify(leaderboard), 'EX', 300 + 3600);
      console.log(`✅ Poll ${pollId} finalized.`);
    } catch (err) {
      console.error(`❌ Error finalizing poll ${pollId}:`, err);
    }
  }

  const PORT = process.env.PORT || 8080;
  app.listen(PORT, () => console.log(`Worker ${process.pid} running on port ${PORT}`));
}
