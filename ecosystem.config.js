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
        IPINFO_TOKEN: "",
        MEASUREMENT_INTERVAL_MIN: 0,
        MEASUREMENT_HW_INTERVAL_SEC: 0,
        TRANSLATION_TIME: "00:00",
        SAVING_TIME: "00:00",
        ACTIVITY_API: "",
        ACTIVITY_REPORT_TIME: "",
        ACTIVITY_REPORT: "Activity/logs/activity-status.json",
        GAME_INACTIVE_AFTER_DAYS: 60,
        GAME_DELETION_GRACE_DAYS: 30,
        GAME_CLEANUP_ENABLED: false,
        GAMES_DIR: "../lean4game/games",
        SESSIONS_API: "",
        GAME_PROTECTED_REPOS: "",
        GAME_TRASH_RETENTION_DAYS: 7,
        CLEANUP_REPORT: "Activity/logs/cleanup-status.json",
        LOGGER_METRICS_HOST: "127.0.0.1",
        LOGGER_METRICS_PORT: 8078
    }
  }]
}
