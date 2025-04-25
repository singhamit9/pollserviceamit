module.exports = {
  apps: [{
    name: 'pollservice',
    script: 'server.js',
    instances: 'max',
    exec_mode: 'cluster',
    env: {
      NODE_ENV: 'production',
      EMQX_HOST: 'MQTT-chat-7c6cdb28f96eeaf6.elb.ap-south-1.amazonaws.com',
      PORT: 8080
    }
  }]
};
