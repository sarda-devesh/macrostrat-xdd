from sqlalchemy import create_engine, inspect, MetaData
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import select as SELECT_STATEMENT
from sqlalchemy import update as UPDATE_STATEMENT
import argparse
import traceback
import os
import pandas as pd
import numpy as np
import requests

def read_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", type=str, required=True, help="The URI to use to connect to the database")
    parser.add_argument("--schema", type=str, required=True, help="The schema to connect to")
    return parser.parse_args()

def load_sqlalchemy(args):
    # Create the engine
    engine = create_engine(args.uri)
    metadata = MetaData(schema = args.schema)
    metadata.reflect(bind=engine)
    Session = sessionmaker(bind=engine)
    current_session = Session()

    return {
        "engine" : engine,
        "metadata" : metadata,
        "session" : current_session,
        "schema" : args.schema
    }

def get_complete_table_name(connection_details, table_name):
    return connection_details["schema"] + "." + table_name

def get_paper_tags(paper_id):
    request_params = {"docid" : paper_id, "fields" : "tags"}
    try:
        get_result = requests.get("https://xdd.wisc.edu/api/articles", params = request_params).json()
    except:
        print("Got except making get request for paper", paper_id)
        return None

    # Make sure the tags fields exist
    if "success" not in get_result:
        print("Missing success from get result for paper", paper_id)
        return None
    
    success_result = get_result["success"]
    if "data" not in success_result:
        print("Missing data from get result for paper", paper_id)
        return None
    
    data_result = success_result["data"]
    if len(data_result) == 0:
        print("Data result is empty for paper", paper_id)
        return None
    
    first_data_result = data_result[0]
    if "tags" not in first_data_result:
        print("Missing tags for paper", paper_id)
        return None
    
    all_tags = first_data_result["tags"]
    if len(all_tags) == 0:
        print("Got empty tags for paper", paper_id)
        return None
    
    return ",".join(all_tags)

def set_paper_tags(connection_details, texts_table, paper_id, paper_tag):
    try:
        # Create the update statement
        text_update_statement = UPDATE_STATEMENT(texts_table)
        text_update_statement = text_update_statement.where(texts_table.c.paper_id == paper_id)
        insert_values = {"xdd_tags" : paper_tag}
        text_update_statement = text_update_statement.values(**insert_values)

        # Run the update statement
        connection_details["session"].execute(text_update_statement)
        connection_details["session"].commit()
        print("For paper id", paper_id, "set paper tag of", paper_tag)
    except:
        print("Failed to set tag", paper_tag, "for paper id", paper_id, "due to error", traceback.format_exc())

def process_all_sources(connection_details):
    # Load the source text
    texts_table_name = get_complete_table_name(connection_details, "source_text")
    texts_table = connection_details["metadata"].tables[texts_table_name]
    text_select_statement = SELECT_STATEMENT(texts_table)
    text_select_statement = text_select_statement.where(texts_table.c.paper_id != None)
    text_select_statement = text_select_statement.where(texts_table.c.xdd_tags == None)

    text_select_result = connection_details["session"].execute(text_select_statement).all()
    for curr_result in text_select_result:
        # Get the paper tag for this paper
        curr_paper_id = str(curr_result._mapping["paper_id"])        
        paper_tag = get_paper_tags(curr_paper_id)

        # Set the paper
        if paper_tag is not None:
            set_paper_tags(connection_details, texts_table, curr_paper_id, paper_tag)

def main():
    # Load the schema
    args = read_args()
    connection_details = load_sqlalchemy(args)

    process_all_sources(connection_details)

if __name__ == "__main__":
    main()