from sqlalchemy import create_engine, inspect, MetaData
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import select as SELECT_STATEMENT
import argparse
import os
import pandas as pd
import numpy as np

def read_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", type=str, required=True, help="The URI to use to connect to the database")
    parser.add_argument("--schema", type=str, required=True, help="The schema to connect to")
    parser.add_argument("--save_dir", type=str, required=True, help="The directory to save the results to")
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

def get_entity_name(connection_details, entity_id):
    # Load the entity table
    entities_table_name = get_complete_table_name(connection_details, "entity")
    entities_table = connection_details["metadata"].tables[entities_table_name]
    entities_select_statement = SELECT_STATEMENT(entities_table)
    entities_select_statement = entities_select_statement.where(entities_table.c.id == entity_id)

    # Run the query
    entities_select_result = connection_details["session"].execute(entities_select_statement).all()
    if len(entities_select_result) == 0:
        raise Exception("Can't find entity with id " + str(entities_select_result))
    
    return entities_select_result[0]._mapping["name"].strip()

def get_relationship_type(connection_details, relationship_type_id):
    # Load the relationship type table
    relationship_table_name = get_complete_table_name(connection_details, "relationship_type")
    relationship_table = connection_details["metadata"].tables[relationship_table_name]
    relationship_select_statement = SELECT_STATEMENT(relationship_table)
    relationship_select_statement = relationship_select_statement.where(relationship_table.c.id == relationship_type_id)

    # Run the query
    relationship_select_result = connection_details["session"].execute(relationship_select_statement).all()
    if len(relationship_select_result) == 0:
        raise Exception("Can't find relationship type with id " + str(relationship_select_result))
    
    return relationship_select_result[0]._mapping["name"].strip()

def get_user_run_relationships(connection_details, save_dir, run_id, source_text_id):
    # Load the source text
    texts_table_name = get_complete_table_name(connection_details, "source_text")
    texts_table = connection_details["metadata"].tables[texts_table_name]
    text_select_statement = SELECT_STATEMENT(texts_table)
    text_select_statement = text_select_statement.where(texts_table.c.id == source_text_id)

    text_select_result = connection_details["session"].execute(text_select_statement).all()
    if len(text_select_result) == 0:
        raise Exception("Can't find text for source id " + str(source_text_id))
    
    # Get the paragraph text details
    source_text = text_select_result[0]._mapping["paragraph_text"]
    source_text_hash = text_select_result[0]._mapping["hashed_text"]

    # Extract the relationship
    relationship_table_name = get_complete_table_name(connection_details, "relationship")
    relationship_table = connection_details["metadata"].tables[relationship_table_name]
    relationship_select_statement = SELECT_STATEMENT(relationship_table)
    relationship_select_statement = relationship_select_statement.where(relationship_table.c.run_id == run_id)

    all_results = []
    all_relationships = connection_details["session"].execute(relationship_select_statement).all()
    for curr_relationship in all_relationships:
        # Extract the fields
        src_entity_id = curr_relationship._mapping["src_entity_id"]
        dst_entity_id = curr_relationship._mapping["dst_entity_id"]
        relationship_type_id = curr_relationship._mapping["relationship_type_id"]
        
        # Get the values from the ids
        src_text = get_entity_name(connection_details, src_entity_id)
        dst_text = get_entity_name(connection_details, dst_entity_id)
        relationship_type = get_relationship_type(connection_details, relationship_type_id)

        # Record this dataset
        all_results.append({
            "doc_id" : source_text_id,
            "title" : source_text_hash,
            "text" : source_text,
            "src" : src_text,
            "dst" : dst_text,
            "type" : relationship_type
        })

    return pd.DataFrame(all_results)

DATASET_SPLIT = [0.8, 0.1, 0.1]
def save_results(combined_df, save_dir):
    # Create output directory if it doesn't exist
    os.makedirs(save_dir, exist_ok=True)

    # Calculate split sizes
    total_rows = len(combined_df)
    train_size = int(0.8 * total_rows)
    test_size = int(0.1 * total_rows)
    valid_size = total_rows - train_size - test_size

    # Split the dataframe
    train_df = combined_df[:train_size]
    test_df = combined_df[train_size:train_size+test_size]
    valid_df = combined_df[train_size+test_size:]

    # Function to save dataframes to CSV files
    def save_to_csv(data, prefix):
        file_names = []
        for i, chunk in enumerate(np.array_split(data, max(1, len(data) // 1000))):
            file_name = f"{prefix}_{i}.csv"
            chunk.to_csv(os.path.join(save_dir, file_name), index=False, sep = '\t')
            file_names.append(file_name)
        return file_names

    # Save each split to CSV files
    train_files = save_to_csv(train_df, 'train')
    test_files = save_to_csv(test_df, 'test')
    valid_files = save_to_csv(valid_df, 'valid')

    # Create text files listing the CSV files for each split
    for split_name, file_list in [('train', train_files), ('test', test_files), ('valid', valid_files)]:
        with open(os.path.join(save_dir, f"{split_name}.txt"), 'w') as f:
            f.write('\n'.join(file_list))

    print(f"Files saved in {save_dir} directory.")

def main():
    # Load the schema
    args = read_args()
    connection_details = load_sqlalchemy(args)

    # Get all of the user runs
    save_dir = "extracted_feedback"
    all_user_runs = get_all_user_runs(connection_details)
    dfs_to_combine = []
    for run_id, source_text_id in all_user_runs:
        feedback_df = get_user_run_relationships(connection_details, save_dir, run_id, source_text_id)
        dfs_to_combine.append(feedback_df)
    combined_df = pd.concat(dfs_to_combine)

    # Save the result in the proper format
    save_results(combined_df, args.save_dir)
    connection_details["session"].close()

if __name__ == "__main__":
    main()