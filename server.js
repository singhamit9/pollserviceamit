
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

  mqttClient.on('offline', () => {
    console.warn('⚠️ MQTT client offline');
    isMqttConnected = false;
  });

  app.post('/managePoll', async (req, res) => {
    const { type, data } = req.body;
    if (!type) return res.status(400).json({ error: 'Invalid payload' });

    try {
      switch (type) {
        case 'CREATE_POLL': {
          const setting_node = req.body.setting_node;
          const course_id = req.body.course_id || '';
          const video_id = req.body.video_id || '';
          const pollIdCreate = req.body.id || (video_id + '' + Date.now());

          const options = {
            option_1: data.option_1,
            option_2: data.option_2,
            option_3: data.option_3,
            option_4: data.option_4,
            option_5: data.option_5,
            option_6: data.option_6,
            answer: data.answer,
            created: Math.floor(Date.now() / 1000),
            delay: Math.floor(Date.now() / 1000) + 1,
            validity: data.validity,
            valid_till: Math.floor(Date.now() / 1000) + parseInt(data.validity || '60'),
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
            duration: data.validity
          });
          redis.expire(`Poll:${pollIdCreate}:meta`, 3600);

          const payload = {
            poll_id: pollIdCreate,
            type: 'poll',
            date: Math.floor(Date.now() / 1000),
            is_active: '1',
            name: req.body.name || 'NT Admin',
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

          console.log(`📡 Publishing Poll to MQTT:
Topic = ${setting_node}
Payload =`, payload);
          mqttClient.publish(setting_node, JSON.stringify(payload), { qos: 1, retain: true }, (err) => {
            if (err) {
              console.error('❌ Error publishing poll to MQTT:', err);
            } else {
              console.log('📡 Poll published successfully via MQTT');
            }
          });

          setTimeout(() => finalizePoll(pollIdCreate), parseInt(data.validity) * 1000);
          res.json({ success: true });
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
      await redis.set(`Poll:${pollId}:final_leaderboard`, JSON.stringify(leaderboard), 'EX', 3600);
      console.log(`✅ Poll ${pollId} finalized.`);
    } catch (err) {
      console.error(`❌ Error finalizing poll ${pollId}:`, err);
    }
  }

  const PORT = process.env.PORT || 8080;
  app.listen(PORT, () => console.log(`Worker ${process.pid} running on port ${PORT}`));
}
