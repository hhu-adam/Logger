"""Logger API; reports can also be served offline with `flask --app api run`."""

import os
import math
from pathlib import Path

from flask import Flask, jsonify, request

from Reporting.service import ReportingService
from state import data_lock, state


def live_snapshot():
    with data_lock:
        return {
            "locations": state.daily_game_user_log[["game", "n"]].to_dict(orient="records"),
            "usage": state.daily_hardware_user_log.to_dict(orient="records"),
            "location_period_start": state.reporting_location_period_start,
            "last_location_measurement": state.reporting_last_location_measurement,
        }


def create_app(reporting=None):
    app = Flask(__name__)
    app.json.sort_keys = False
    root = Path(__file__).resolve().parent
    service = reporting or ReportingService(
        os.getenv("LOGGER_LOCATION_LOG_DIR", root / "Location/logs"),
        os.getenv("LOGGER_USAGE_LOG_DIR", root / "Usage/logs"),
        os.getenv("LOGGER_ENVIRONMENT", "production"),
        snapshot=live_snapshot,
    )

    @app.get("/usage/latest")
    def get_latest_max_user_and_hw_measurement():
        from scheduler import use_meter
        latest = use_meter.get_measurement().to_dict(orient="records")[0]
        latest = {
            key: None if isinstance(value, float) and not math.isfinite(value) else value
            for key, value in latest.items()
        }
        return jsonify(latest)

    @app.get("/reporting/metadata")
    def reporting_metadata():
        return jsonify(service.metadata())

    @app.get("/reporting/report")
    def reporting_report():
        try:
            return jsonify(service.report(request.args.get("from"), request.args.get("to"), request.args.get("bucket", "day")))
        except ValueError as error:
            return jsonify({"error": str(error)}), 400

    return app


app = create_app()
