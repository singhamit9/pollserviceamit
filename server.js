const express = require('express');
const Redis = require('ioredis');
const mqtt = require('mqtt');
const cluster = require('cluster');
const os = require('os');
const cors = require('cors');

const app = express();
app.use(express.json());
app.use(cors());

const redis = new Redis({
  host: 'your-redis-endpoint.amazonaws.com',  // <<== CHANGE THIS
  port: 6379,
  tls: {}
});

const MQTT_HOST = 'mqtt://your-emqx-endpoint.com';  // <<== CHANGE THIS
const MQTT_OPTIONS = {
  clientId: 'pollservice_' + Math.random().toString(16).substr(2, 8),
  clean: true,
  reconnectPeriod: 1000,
};

const client = mqtt.connect(MQTT_HOST, MQTT_OPTIONS);

client.on('connect', () => {
  console.log('✅ Connected to MQTT broker');
});

client.on('error', (err) => {
  console.error('❌ MQTT connection error:', err);
});

const numCPUs = os.cpus().length;

if (cluster.isPrimary) {
  console.log(`Master ${process.pid} is running`);
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }
} else {

  app.get('/poll/health', (req, res) => {
    res.send('OK');
  });

  app.post('/managePoll', async (req, res) => {
    const { type, data, setting_node, video_id, course_id, name, id } = req.body;

    if (!type || !data) {
      return res.status(400).json({ error: 'Invalid payload' });
    }

    try {
      if (type === 'CREATE_POLL') {
        const createdTime = Math.floor(Date.now() / 1000);
        const delay = parseInt(data.delay || '1');
        const validity = parseInt(data.validity || '60');

        const poll_id = id || `${video_id}${createdTime}`;

        const pollData = {
          question: data.question || '',
          option_1: data.option_1 || '',
          option_2: data.option_2 || '',
          option_3: data.option_3 || '',
          option_4: data.option_4 || '',
          answer: data.answer || '0',
          created: createdTime,
          delay: createdTime + delay,
          validity,
          valid_till: createdTime + validity,
          disable_result: 0,
          status: '1',
          video_id: video_id || '',
          attempt_1: 0,
          attempt_2: 0,
          attempt_3: 0,
          attempt_4: 0,
          id: poll_id,
          firebase_key: poll_id,
          poll_key: poll_id,
          poll_id: poll_id,
        };

        // Store metadata
        await redis.hmset(`Poll:${poll_id}:meta`, pollData);
        await redis.expire(`Poll:${poll_id}:meta`, validity);

        // Publish over MQTT
        const payload = {
          poll_id,
          type: 'poll',
          date: createdTime,
          is_active: '1',
          name: name || '',
          profile_picture: '',
          pin: '0',
          user_id: '',
          platform: req.body.plateform || '',
          course_id: course_id || '',
          video_id: video_id || '',
          id: poll_id,
          message: pollData
        };

        client.publish(setting_node, JSON.stringify(payload), { qos: 1 }, (err) => {
          if (err) {
            console.error('❌ MQTT publish error:', err);
          } else {
            console.log('📡 Poll published successfully via MQTT');
          }
        });

        return res.json({ success: true, message: 'Poll added successfully' });
      }

      if (type === 'UPDATE_POLL') {
        const { poll_id, user_id, attempted, timeleft } = data;

        if (!poll_id || !user_id) {
          return res.status(400).json({ error: 'poll_id and user_id required' });
        }

        const userKey = `Poll:${poll_id}:user:${user_id}`;
        const answeredSet = `Poll:${poll_id}:users_answered`;

        const pollMeta = await redis.hgetall(`Poll:${poll_id}:meta`);
        if (!pollMeta.correct_option) {
          return res.status(404).json({ error: 'Poll not found' });
        }

        const isCorrect = attempted == pollMeta.answer;

        const added = await redis.sadd(answeredSet, user_id);
        if (added === 0) {
          return res.status(200).send('Already answered');
        }

        const pipeline = redis.pipeline();
        pipeline.hincrby(`Poll:${poll_id}:votes`, attempted, 1);
        pipeline.hmset(userKey, {
          selected_option: attempted,
          is_correct: isCorrect,
          response_time: timeleft
        });

        if (isCorrect) {
          pipeline.zadd(`Poll:${poll_id}:leaderboard`, timeleft, user_id);
        }

        pipeline.expire(userKey, pollMeta.validity || 3600);
        await pipeline.exec();

        return res.json({ success: true, message: 'Vote recorded' });
      }

      if (type === 'GET_POLL') {
        const { poll_id } = data;
        const pollMeta = await redis.hgetall(`Poll:${poll_id}:meta`);
        if (!pollMeta) {
          return res.status(404).send('Poll not found');
        }
        return res.json({ poll: pollMeta });
      }

      if (type === 'GET_RESULT') {
        const { poll_id } = data;
        const votes = await redis.hgetall(`Poll:${poll_id}:votes`);
        if (!votes) {
          return res.status(404).send('Votes not found');
        }
        return res.json({ votes });
      }

      if (type === 'GET_LEADERBOARD' || type === 'GET_LEADERBOARD_VIDEOWISE') {
        const { poll_id } = data;
        const leaderboardData = await redis.get(`Poll:${poll_id}:final_leaderboard`);
        if (!leaderboardData) {
          return res.status(404).send('Leaderboard not found');
        }
        return res.json({ leaderboard: JSON.parse(leaderboardData) });
      }

      return res.status(400).send('Unknown type');

    } catch (err) {
      console.error('❌ Error handling managePoll:', err);
      return res.status(500).send('Internal server error');
    }
  });

  const PORT = process.env.PORT || 8080;
  app.listen(PORT, () => {
    console.log(`Worker ${process.pid} running on port ${PORT}`);
  });
}
