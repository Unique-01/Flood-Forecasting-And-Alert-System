from flask import Flask, jsonify
from flask_cors import CORS
from sqlalchemy import create_engine
import pandas as pd
import json
import os
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, filename='app.log', filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s')

current_file_path = Path(__file__).resolve()
parent_path = current_file_path.parents[1]

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "http://localhost:5173"}})

db_path = parent_path / "data/flood_data.db"
geojson_path = parent_path / "data/lagos_landuse_cleaned_valid.geojson"
engine = create_engine(f"sqlite:///{db_path}")

@app.route('/api/health')
def health():
    logging.info("Health check requested")
    return jsonify({"status": "healthy"})

@app.route('/api/socioeconomic')
def get_socioeconomic():
    try:
        df = pd.read_sql("SELECT state, landuse_type, area_sqm FROM socioeconomic;", engine)
        logging.info("Socioeconomic data fetched successfully")
        return jsonify(df.to_dict(orient='records'))
    except Exception as e:
        logging.error(f"Socioeconomic fetch error: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/sentinel_features')
def get_sentinel():
    try:
        df = pd.read_sql("SELECT region, week_start_date, image_count FROM sentinel_features;", engine)
        logging.info("Sentinel data fetched successfully")
        return jsonify(df.to_dict(orient='records'))
    except Exception as e:
        logging.error(f"Sentinel fetch error: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/weather_features')
def get_weather():
    try:
        df = pd.read_sql("SELECT city, window_start_date, avg_precipitation_7d, avg_temperature_7d, avg_humidity_7d, avg_precipitation_30d FROM weather_features;", engine)
        logging.info("Weather data fetched successfully")
        return jsonify(df.to_dict(orient='records'))
    except Exception as e:
        logging.error(f"Weather fetch error: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/landuse/lagos')
def get_landuse():
    try:
        if not os.path.exists(geojson_path):
            logging.error(f"GeoJSON file not found: {geojson_path}")
            return jsonify({"error": "GeoJSON file not found"}), 404
        with open(geojson_path, 'r') as f:
            data = json.load(f)
        logging.info(f"GeoJSON data loaded successfully from {geojson_path}")
        # Validate GeoJSON structure
        if not isinstance(data, dict) or data.get('type') != 'FeatureCollection':
            logging.error("Invalid GeoJSON: Not a FeatureCollection")
            return jsonify({"error": "Invalid GeoJSON format"}), 400
        return data  # Return dict directly to avoid double jsonify
    except json.JSONDecodeError as e:
        logging.error(f"GeoJSON decode error: {str(e)}")
        return jsonify({"error": f"Invalid GeoJSON: {str(e)}"}), 400
    except Exception as e:
        logging.error(f"Land use fetch error: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    logging.info("Starting Flask server")
    app.run(debug=True, host='0.0.0.0', port=5000)