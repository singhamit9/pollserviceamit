module.exports = {
  apps : [{
    name: "pollserv",
    script: "./server.js",
    instances: "max",
    exec_mode: "cluster",
    watch: false,
    env: {
      PORT: 8080,
      NODE_ENV: "production"
    }
  }]
}
