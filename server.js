
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

  app.post('/managePoll', async (req, res) => {
    const { type, data } = req.body;
    if (!type) return res.status(400).json({ error: 'Invalid payload' });

    try {
      switch (type) {
        case 'CREATE_POLL':
          const pollIdCreate = data?.poll_id || req.body.poll_id;
          const setting_node = data?.setting_node || req.body.setting_node;
          const duration = data?.validity || req.body.validity;

          await redis.hmset(`Poll:${pollIdCreate}:meta`, {
            question: data?.question || req.body.question,
            options: JSON.stringify(data?.options || req.body.options),
            correct_option: data?.correct_option || req.body.correct_option,
            start_time: Date.now(),
            duration
          });
          redis.expire(`Poll:${pollIdCreate}:meta`, 3600);

          const payload = {
            poll_id: pollIdCreate,
            question: data?.question || req.body.question,
            options: data?.options || req.body.options,
            duration
          };

          const mqttClient = mqtt.connect(`mqtt://${EMQX_HOST}:1883`, {
            clientId: 'PollService-' + Math.random().toString(16).substr(2, 8),
            clean: true,
            reconnectPeriod: 1000
          });

          mqttClient.on('connect', () => {
            mqttClient.publish(setting_node, JSON.stringify(payload), { qos: 1, retain: true }, (err) => {
              if (err) console.error('❌ Error publishing:', err);
              mqttClient.end();
            });
          });

          setTimeout(() => finalizePoll(pollIdCreate), duration * 1000);
          res.json({ success: true });
          break;

        case 'UPDATE_POLL':
          const pollIdUpdate = data?.poll_id || req.body.poll_id;
          const userIdUpdate = data?.user_id || req.body.user_id;
          const selectedOption = data?.attempted || req.body.attempted;
          const responseTime = data?.timeleft || req.body.timeleft;

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
          pipeline.expire(userKey, 3600);
          await pipeline.exec();

          res.json({ success: true });
          break;

        case 'GET_POLL':
          const pollIdGet = data?.poll_id || req.body.poll_id;
          const userIdGet = data?.user_id || req.body.user_id;
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

        case 'GET_LEADERBOARD':
        case 'GET_LEADERBOARD_VIDEOWISE':
          const pollIdLeader = data?.poll_id || req.body.poll_id;
          const leaderboardData = await redis.get(`Poll:${pollIdLeader}:final_leaderboard`);
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

  const PORT = process.env.PORT || 80;
  app.listen(PORT, () => console.log(`Worker ${process.pid} running on port ${PORT}`));
}
