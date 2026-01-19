module.exports = {
  apps : [{
    name   : "logger",
    script : "./scheduler.py", 
    interpreter : "./env/bin/python3",
    env: {
        IPS_LOG: "",
        API: "",
        HARDWARE_SCRIPT: "",
        MEASUREMENT_INTERVAL_MIN: 0,
        MEASUREMENT_HW_INTERVAL_SEC: 0,
        TRANSLATION_TIME: "",
        SAVING_TIME: ""
    }
  }]
}
