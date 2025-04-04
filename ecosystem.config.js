module.exports = {
  apps : [{
    name   : "Logger",
    script : "./scheduler.py", 
    interpreter : "./env/bin/python3",
    env: {
        IPS_LOG: ""
    }
  }]
}
