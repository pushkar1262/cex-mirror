// PM2 process config for cex-mirror.
// Usage:
//   pm2 start ecosystem.config.js
//   pm2 logs cex-mirror
//   pm2 restart cex-mirror
//   pm2 stop cex-mirror        # SIGINT -> graceful shutdown (cancel_on_shutdown)
module.exports = {
  apps: [
    {
      name: "cex-mirror",
      cwd: "/home/user/Projects/exchange/cex-mirror",
      script: ".venv/bin/python",
      interpreter: "none",              // run the venv python directly, not via Node
      args: "-m cex_mirror config.yaml",
      env: {
        LOG_LEVEL: "INFO",              // DEBUG for per-order logs
      },
      autorestart: true,
      max_restarts: 10,
      kill_timeout: 10000,              // give cancel_on_shutdown time to finish on stop
      time: true,                       // prefix logs with timestamps
    },
  ],
};