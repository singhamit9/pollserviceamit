module.exports = {
  apps: [{
    name: 'pollservice',
    script: 'server.js',
    instances: 'max',
    exec_mode: 'cluster',
    env: {
      NODE_ENV: 'production',
      EMQX_HOST: 'your-emqx-endpoint.com',
      PORT: 8080
    }
  }]
};