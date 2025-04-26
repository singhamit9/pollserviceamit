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
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }
  cluster.on('exit', (worker, code, signal) => {
    console.log(`Worker ${worker.process.pid} died. Restarting...`);
    cluster.fork();
  });
} else {
  const app = express();
  app.use(express.json());
  app.use(cors());

  // ALB Health Check Route
  app.get('/poll/health', (req, res) => {
    res.status(200).send('OK');
  });

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

  app.post('/managePoll', async (req, res) => {
    const { type, data } = req.body;
    if (!type || !data) return res.status(400).json({ error: 'Invalid payload' });

    try {
      switch (type) {
        case 'CREATE_POLL':
          const pollId = data.poll_id;
          const setting_node = data.setting_node;
          const duration = data.validity;

          await redis.hmset(`Poll:${pollId}:meta`, {
            question: data.question,
            options: JSON.stringify(data.options),
            correct_option: data.correct_option,
            start_time: Date.now(),
            duration
          });
          redis.expire(`Poll:${pollId}:meta`, 3600);

          const payload = {
            poll_id: pollId,
            question: data.question,
            options: data.options,
            duration
          };

          const mqttClient = mqtt.connect(`mqtt://${EMQX_HOST}:1883`, {
            clientId: 'PollService-' + Math.random().toString(16).substr(2, 8),
            clean: true,
            reconnectPeriod: 1000
          });

          mqttClient.on('connect', () => {
            mqttClient.publish(setting_node, JSON.stringify(payload), { qos: 2, retain: false }, (err) => {
              if (err) console.error('❌ Error publishing:', err);
              mqttClient.end();
            });
          });

          setTimeout(() => finalizePoll(pollId), duration * 1000);
          res.json({ success: true });
          break;

        case 'UPDATE_POLL':
          const { user_id, selected_option, poll_id, response_time } = data;
          const userKey = `Poll:${poll_id}:user:${user_id}`;
          const answeredSet = `Poll:${poll_id}:users_answered`;
          const pollMeta = await redis.hgetall(`Poll:${poll_id}:meta`);

          if (!pollMeta.correct_option) return res.status(404).send('Poll not found');

          const isCorrect = selected_option === pollMeta.correct_option;
          const added = await redis.sadd(answeredSet, user_id);
          if (added === 0) return res.status(200).send('Already answered');

          const pipeline = redis.pipeline();
          pipeline.hincrby(`Poll:${poll_id}:votes`, selected_option, 1);
          pipeline.hmset(userKey, {
            selected_option,
            is_correct: isCorrect,
            response_time
          });
          if (isCorrect) pipeline.zadd(`Poll:${poll_id}:leaderboard`, response_time, user_id);
          pipeline.expire(userKey, 3600);
          await pipeline.exec();

          res.json({ success: true });
          break;

        case 'GET_POLL':
          const pollData = await redis.hgetall(`Poll:${data.poll_id}:meta`);
          if (!pollData) return res.status(404).send('Poll not found');
          res.json({ poll: pollData });
          break;

        case 'GET_LEADERBOARD':
        case 'GET_LEADERBOARD_VIDEOWISE':
          const leaderboardData = await redis.get(`Poll:${data.poll_id}:final_leaderboard`);
          if (!leaderboardData) return res.status(404).json({ error: 'Leaderboard not ready' });
          res.json({ leaderboard: JSON.parse(leaderboardData) });
          break;

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
