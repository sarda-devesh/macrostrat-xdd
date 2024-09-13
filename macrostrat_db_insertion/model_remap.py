from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
import sqlalchemy
from sqlalchemy.orm import declarative_base
from sqlalchemy import text
import json
import traceback
import requests
from datetime import datetime, timezone

from re_detail_adder import *

def load_engine(config_file):
    # Load the config file
    with open(config_file, 'r') as reader:
        config = json.load(reader)

    # Create the app 
    app = Flask(__name__)
    url_object = sqlalchemy.URL.create(
        "postgresql",
        username = config["username"],
        password = config["password"],  
        host = config["host"],
        port = config["port"],
        database = config["database"],
    )

    # Create the db
    engine = sqlalchemy.create_engine(url_object)
    return engine

# Connect to the database
MAX_TRIES = 5
CONFIG_FILE_PATH = "actual_macrostrat.json"
engine = load_engine(CONFIG_FILE_PATH)

def extract_runs():
    query_to_run = """
    SELECT run_id, model_id
    FROM macrostrat_kg_new.metadata;
    """
    runs_by_model = {}
    with engine.connect() as connection:
        result = connection.execute(text(query_to_run))
        for row in result:
            metadata_row = row._mapping
            model_id, run_id = metadata_row["model_id"], metadata_row["run_id"]
            if model_id not in runs_by_model:
                runs_by_model[model_id] = []
            runs_by_model[model_id].append(run_id)
    
    save_path = os.path.join("temp_data", "all_runs.json")
    with open(save_path, 'w+') as writer:
        json.dump(runs_by_model, writer, indent = 4)

def extract_model_details():
    read_path = os.path.join("temp_data", "all_runs.json")
    with open(read_path, 'r') as reader:
        runs_by_model = json.load(reader)
    
    updated_map = {}
    for key_name in runs_by_model.keys():
        key_parts = key_name.split("_")
        model_name = "_".join(key_parts[ : -1]).strip()
        model_version = key_parts[-1].strip()
        updated_map[key_name] = {
            "model_name" : model_name,
            "model_version" : model_version,
            "all_runs" : runs_by_model[key_name]
        }
    
    save_path = os.path.join("temp_data", "all_runs_with_name.json")
    with open(save_path, 'w+') as writer:
        json.dump(updated_map, writer, indent = 4)

request_url = "http://127.0.0.1:9543/record_run"
def perform_test():
    read_path = os.path.join("temp_data", "all_runs_with_name.json")
    with open(read_path, 'r') as reader:
        runs_by_model = json.load(reader)
    
    for model_id in runs_by_model.keys():
        model_details = runs_by_model[model_id]
        model_name, model_version = model_details["model_name"], model_details["model_version"]
        for run_id in model_details["all_runs"]:
            request_data = {
                "model_name" : model_name,
                "model_version" : model_version,
                "run_id" : run_id,
                "extraction_pipeline_id" : "0",
            }
            response = requests.post(url = request_url, json = request_data)

if __name__ == "__main__":
    # extract_runs()
    # extract_model_details()
    perform_test()