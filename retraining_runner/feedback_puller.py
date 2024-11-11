from sqlalchemy import create_engine, inspect, MetaData
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import select as SELECT_STATEMENT
import argparse

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

def get_all_user_runs(connection_details):
    # Create the query to get all the user runs
    runs_table_name = get_complete_table_name(connection_details, "all_runs")
    runs_table = connection_details["metadata"].tables[runs_table_name]
    users_run_select_statement = SELECT_STATEMENT(runs_table)
    users_run_select_statement = users_run_select_statement.where(runs_table.c.user_id != None)

    # Run the query and get the results
    users_run_result = connection_details["session"].execute(users_run_select_statement).all()
    all_user_runs = []
    for current_row in users_run_result:
        current_row = current_row._mapping
        all_user_runs.append((current_row["id"], current_row["source_text_id"]))
    
    return all_user_runs

def get_user_run_relationships(connection_details, save_dir, run_id, source_text_id):
    # Load the source text
    texts_table_name = get_complete_table_name(connection_details, "source_text")
    texts_table = connection_details["metadata"].tables[texts_table_name]
    text_select_statement = SELECT_STATEMENT(texts_table)
    text_select_statement = text_select_statement.where(texts_table.c.id == source_text_id)

    text_select_result = connection_details["session"].execute(text_select_statement).all()
    if len(text_select_result) == 0:
        raise Exception("Can't find text for source id " + str(source_text_id))
    
    source_text = text_select_result[0]._mapping["paragraph_text"]
    print(source_text_id, source_text)

    # Extract the relationship
    relationship_table_name = get_complete_table_name(connection_details, "relationship")
    relationship_table = connection_details["metadata"].tables[relationship_table_name]
    relationship_select_statement = SELECT_STATEMENT(relationship_table)
    relationship_select_statement = relationship_select_statement.where(relationship_table.c.run_id == run_id)

    all_relationships = connection_details["session"].execute(relationship_select_statement).all()
    for curr_relationship in all_relationships:
        print(curr_relationship._mapping)
        break

def main():
    # Load the schema
    args = read_args()
    connection_details = load_sqlalchemy(args)

    # Get all of the user runs
    save_dir = "extracted_feedback"
    all_user_runs = get_all_user_runs(connection_details)
    for run_id, source_text_id in all_user_runs:
        get_user_run_relationships(connection_details, save_dir, run_id, source_text_id)
        break

    connection_details["session"].close()

if __name__ == "__main__":
    main()