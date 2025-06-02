module.exports = {
  apps : [{
    name   : "logger",
    script : "./scheduler.py", 
    interpreter : "./env/bin/python3",
    env: {
        IPS_LOG: "",
        API: "",
        MEASUREMENT_INTERVAL_MIN: 0,
        TRANSLATION_TIME: ""
    }
  }]
}
