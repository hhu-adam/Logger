module.exports = {
  apps : [{
    name   : "Logger",
    script : "./Logging/Location/scheduler.py", 
    interpreter : "./env/bin/python3",
    env: {
        PORT: 8050,
        SYS_LOG: "",
        PM2_LOG: "",
        LOG_DIR: "",
        LOC_LOG: "",
        LOC_LOG_START: ""
    }
  }]
}
