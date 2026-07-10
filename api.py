from flask import Flask, jsonify
from Usage.measurement.measurement import UsageMeter
from state import data_lock, state

app = Flask(__name__)
use_meter = UsageMeter()

@app.route("/usage/latest", methods=["GET"])
def get_latest_max_user_and_hw_measurement():
    #with data_lock:
    #    if state.daily_hardware_user_log.empty:
    #        return jsonify({"error": "No data yet"}), 404
    # retrieve latest entry in dataframe
    
    latest = use_meter.get_measurement()    #state.daily_hardware_user_log.iloc[-1].to_dict()
    return jsonify(latest)