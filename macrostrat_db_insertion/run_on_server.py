from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
import sqlalchemy
from sqlalchemy.orm import declarative_base
from sqlalchemy import text
import json
import traceback
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

def main():
    query_to_run = """
    WITH ids_to_keep AS (
        SELECT weaviate_id, MIN(source_id::text) AS keep_id
        FROM macrostrat_kg_new.sources AS sources
        GROUP BY weaviate_id
    )

    SELECT *
    FROM macrostrat_kg_new.sources AS sources
    WHERE source_id::text IN (
        SELECT keep_id FROM ids_to_keep
    )
    """

    save_dir = "all_sources"
    with engine.connect() as connection:
        result = connection.execute(text(query_to_run))
        for row in result:
            # Don't process example row
            metadata_row = dict(row._mapping)
            
            source_id = str(metadata_row.pop('source_id'))
            metadata_row.pop("run_id")
            source_id = source_id.replace("-", "_")
            save_path = os.path.join(save_dir, source_id + ".json")

            with open(save_path, "w+") as writer:
                json.dump(metadata_row, writer, indent = 4)

if __name__ == "__main__":
    main()