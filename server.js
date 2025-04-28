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

  // ALB Health Check Route
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

  // Connect MQTT once per worker
  const mqttClient = mqtt.connect(`mqtt://${EMQX_HOST}:1883`, {
    clientId: 'PollService-' + Math.random().toString(16).substr(2, 8),
    clean: true,
    reconnectPeriod: 1000
  });

  mqttClient.on('connect', () => {
    console.log('✅ Connected to MQTT broker (EMQX)');
  });

  mqttClient.on('error', (err) => {
    console.error('❌ MQTT connection error:', err);
  });

  app.post('/managePoll', async (req, res) => {
    const { type, data, setting_node, id, course_id, plateform, video_id, name } = req.body;
    if (!type || !data) return res.status(400).json({ error: 'Invalid payload' });

    try {
      if (type === 'CREATE_POLL') {
        const pollId = id || Date.now().toString();
        const startTime = Math.floor(Date.now() / 1000);
        const validity = parseInt(data.validity || 60);
        const delay = parseInt(data.delay || 1);
        const videoId = video_id || '';

        const message = {
          question: data.question || '',
          option_1: data.option_1 || '',
          option_2: data.option_2 || '',
          option_3: data.option_3 || '',
          option_4: data.option_4 || '',
          option_5: data.option_5 || '',
          option_6: data.option_6 || '',
          answer: data.answer || '0',
          created: startTime,
          delay: delay,
          validity: validity,
          valid_till: startTime + validity,
          disable_result: 0,
          status: '1',
          video_id: videoId,
          attempt_1: 0,
          attempt_2: 0,
          attempt_3: 0,
          attempt_4: 0,
          id: pollId,
          firebase_key: pollId,
          poll_key: pollId,
          poll_id: pollId
        };

        const payload = {
          poll_id: pollId,
          type: 'poll',
          date: startTime,
          is_active: '1',
          name: name || '',
          profile_picture: '',
          pin: '0',
          user_id: '',
          platform: plateform || '',
          course_id: course_id || '',
          video_id: videoId,
          id: pollId,
          message: message
        };

        // Save Poll metadata into Redis
        await redis.hmset(`Poll:${pollId}:meta`, {
          question: message.question,
          correct_option: message.answer,
          start_time: startTime,
          duration: validity,
          video_id: videoId,
          options: JSON.stringify({
            1: data.option_1 || '',
            2: data.option_2 || '',
            3: data.option_3 || '',
            4: data.option_4 || '',
            5: data.option_5 || '',
            6: data.option_6 || ''
          })
        });

        // TTL based on validity
        await redis.expire(`Poll:${pollId}:meta`, validity);

        console.log('📡 Publishing Poll to MQTT:\nTopic =', setting_node, '\nPayload =', payload);

        mqttClient.publish(setting_node, JSON.stringify(payload), { qos: 1, retain: false }, (err) => {
          if (err) console.error('❌ Error publishing to MQTT:', err);
          else console.log('📡 Poll published successfully via MQTT');
        });

        // Schedule finalization
        setTimeout(() => finalizePoll(pollId), validity * 1000);

        return res.json({ success: true, message: 'Poll added successfully' });
      }

      if (type === 'UPDATE_POLL') {
        const pollId = data?.poll_id || req.body.poll_id;
        const userId = data?.user_id || req.body.user_id;
        const selectedOption = data?.attempted || req.body.attempted;
        const responseTime = data?.timeleft || req.body.timeleft;

        const userKey = `Poll:${pollId}:user:${userId}`;
        const answeredSet = `Poll:${pollId}:users_answered`;
        const pollMeta = await redis.hgetall(`Poll:${pollId}:meta`);
        if (!pollMeta.correct_option) return res.status(404).send('Poll not found');

        const isCorrect = selectedOption === pollMeta.correct_option;
        const added = await redis.sadd(answeredSet, userId);
        if (added === 0) return res.status(200).send('Already answered');

        const pipeline = redis.pipeline();
        pipeline.hincrby(`Poll:${pollId}:votes`, selectedOption, 1);
        pipeline.hmset(userKey, {
          selected_option: selectedOption,
          is_correct: isCorrect,
          response_time: responseTime
        });
        if (isCorrect) pipeline.zadd(`Poll:${pollId}:leaderboard`, responseTime, userId);
        pipeline.expire(userKey, parseInt(pollMeta.duration || 3600));
        await pipeline.exec();

        return res.json({ success: true, message: 'Vote recorded' });
      }

      if (type === 'GET_POLL') {
        const pollId = data?.poll_id || req.body.poll_id;
        const userId = data?.user_id || req.body.user_id;
        const pollData = await redis.hgetall(`Poll:${pollId}:meta`);
        if (!pollData || Object.keys(pollData).length === 0) return res.status(404).send('Poll not found');

        if (pollData.options) {
          try {
            pollData.options = JSON.parse(pollData.options);
          } catch (err) {
            console.error('Error parsing options JSON:', err);
            pollData.options = {};
          }
        }

        let myAnswer = null;
        if (userId) {
          const userVote = await redis.hgetall(`Poll:${pollId}:user:${userId}`);
          if (userVote && userVote.selected_option) {
            myAnswer = userVote.selected_option;
          }
        }

        res.json({ poll: pollData, my_answer: myAnswer });
      }

      if (type === 'GET_RESULT') {
        const pollId = data?.poll_id || req.body.poll_id;
        const votes = await redis.hgetall(`Poll:${pollId}:votes`);
        res.json({ votes });
      }

      if (type === 'GET_LEADERBOARD' || type === 'GET_LEADERBOARD_VIDEOWISE') {
        const pollId = data?.poll_id || req.body.poll_id;
        const leaderboardData = await redis.get(`Poll:${pollId}:final_leaderboard`);
        if (!leaderboardData) return res.status(404).json({ error: 'Leaderboard not ready' });
        res.json({ leaderboard: JSON.parse(leaderboardData) });
      }

    } catch (err) {
      console.error('❌ Error handling managePoll:', err);
      res.status(500).send('Internal error');
    }
  });

  async function finalizePoll(pollId) {
    try {
      const top10 = await redis.zrange(`Poll:${pollId}:leaderboard`, 0, 9, 'WITHSCORES');
      const leaderboard = [];
      for (let i = 0; i < top10.length; i += 2) {
        leaderboard.push({ user_id: top10[i], time: parseFloat(top10[i + 1]) });
      }
      await redis.set(`Poll:${pollId}:final_leaderboard`, JSON.stringify(leaderboard), 'EX', 3600);
      console.log(`✅ Poll ${pollId} finalized.`);
    } catch (err) {
      console.error(`❌ Error finalizing poll ${pollId}:`, err);
    }
  }

  const PORT = process.env.PORT || 8080;
  app.listen(PORT, () => console.log(`Worker ${process.pid} running on port ${PORT}`));
}
