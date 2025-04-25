const express = require('express');
const Redis = require('ioredis');
const cors = require('cors');
const mqtt = require('mqtt');
const cluster = require('cluster');
const os = require('os');

const numCPUs = os.cpus().length;
const EMQX_HOST = process.env.EMQX_HOST || 'your-emqx-endpoint.com';

if (cluster.isMaster) {
  for (let i = 0; i < numCPUs; i++) cluster.fork();
  cluster.on('exit', (_, __, ___) => cluster.fork());
} else {
  const app = express();
  app.use(express.json());
  app.use(cors());

  const redis = new Redis({
    host: 'your-redis-endpoint.amazonaws.com',
    port: 6379,
    tls: {}
  });

  redis.ping().then(r => console.log('✅ Redis connected')).catch(err => {
    console.error('❌ Redis failed:', err);
    process.exit(1);
  });

  const mqttClient = mqtt.connect(`mqtt://${EMQX_HOST}:1883`, {
    clientId: 'PollService-' + Math.random().toString(16).substr(2, 8),
    clean: true,
    reconnectPeriod: 1000
  });

  mqttClient.on('connect', () => console.log('✅ Connected to EMQX (TCP)'));
  mqttClient.on('error', err => console.error('❌ MQTT Error:', err));

  app.get('/poll/health', (req, res) => res.status(200).send('OK'));

  app.post('/poll/:id/initiate', async (req, res) => {
    const pollId = req.params.id;
    const { classId, question, options, correct_option, duration } = req.body;

    if (!question || !options || !correct_option || !classId || !duration)
      return res.status(400).json({ error: 'Missing fields' });

    await redis.hmset(`Poll:${pollId}:meta`, {
      question,
      options: JSON.stringify(options),
      correct_option,
      start_time: Date.now(),
      duration
    });
    redis.expire(`Poll:${pollId}:meta`, 3600);

    mqttClient.publish(`class/${classId}/polls`, JSON.stringify({
      poll_id: pollId, question, options, duration
    }), { qos: 0, retain: false });

    setTimeout(() => finalizePoll(pollId), duration * 1000);
    res.json({ success: true, message: 'Poll published' });
  });

  app.post('/poll/:id/vote', async (req, res) => {
    const pollId = req.params.id;
    const { userId, selectedOption, responseTime } = req.body;
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
    pipeline.expire(userKey, 3600);
    await pipeline.exec();

    res.send('Vote recorded');
  });

  async function finalizePoll(pollId) {
    const top10 = await redis.zrange(`Poll:${pollId}:leaderboard`, 0, 9, 'WITHSCORES');
    const leaderboard = [];
    for (let i = 0; i < top10.length; i += 2)
      leaderboard.push({ userId: top10[i], time: parseFloat(top10[i + 1]) });

    await redis.set(`Poll:${pollId}:final_leaderboard`, JSON.stringify(leaderboard), 'EX', 3600);

    const votes = await redis.hgetall(`Poll:${pollId}:votes`);
    const meta = await redis.hget(`Poll:${pollId}:meta`, 'correct_option');

    await redis.set(`Poll:${pollId}:final_result`, JSON.stringify({
      votes,
      correct_option: meta
    }), 'EX', 3600);

    console.log(`✅ Poll ${pollId} finalized`);
  }

  app.post('/poll/:id/finalize', async (req, res) => {
    try {
      await finalizePoll(req.params.id);
      res.json({ success: true });
    } catch (err) {
      console.error('❌ Finalization error:', err);
      res.status(500).send('Failed to finalize poll');
    }
  });

  app.get('/poll/:id/result', async (req, res) => {
    const pollId = req.params.id;
    const { userId } = req.query;

    const data = await redis.get(`Poll:${pollId}:final_result`);
    const user = await redis.hgetall(`Poll:${pollId}:user:${userId}`);

    if (!data) return res.status(404).send('Result not ready');

    const result = JSON.parse(data);
    res.json({
      votes: result.votes,
      correct_option: result.correct_option,
      user_selection: user.selected_option,
      is_correct: user.is_correct === 'true'
    });
  });

  app.get('/poll/:id/leaderboard', async (req, res) => {
    const pollId = req.params.id;
    const data = await redis.get(`Poll:${pollId}:final_leaderboard`);
    if (!data) return res.status(404).json({ error: 'Leaderboard not ready' });
    res.json({ leaderboard: JSON.parse(data) });
  });

  app.listen(8080, () => console.log(`Worker ${process.pid} running`));
}
