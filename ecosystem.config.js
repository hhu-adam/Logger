module.exports = {
  apps : [{
    name   : "logger",
    script : "./main.py", 
    interpreter : "./env/bin/python3",
    env: {
        IPS_LOG: "",
        API: "",
        HARDWARE_SCRIPT: "",
        HARDWARE_INFO_FILE: "",
        CPU_SCRIPT: "",
        MEASUREMENT_INTERVAL_MIN: 0,
        MEASUREMENT_HW_INTERVAL_SEC: 0,
        TRANSLATION_TIME: "00:00",
        SAVING_TIME: "00:00"
    }
  }]
}
