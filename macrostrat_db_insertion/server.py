from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
import sqlalchemy
from sqlalchemy.dialects.postgresql import insert as INSERT_STATEMENT
from sqlalchemy import select as SELECT_STATEMENT
from sqlalchemy.orm import declarative_base
import json
import traceback
import requests
from datetime import datetime, timezone
import os

from re_detail_adder import *

def load_flask_app(schema_name):
    # Create the app
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = os.environ['uri']

    # Create the db
    Base = declarative_base(metadata = sqlalchemy.MetaData(schema = schema_name))
    db = SQLAlchemy(model_class=Base)
    db.init_app(app)
    with app.app_context():
        db.reflect()

    return app, db

# Connect to the database
MAX_TRIES = 5
SCHEMA_NAME = os.environ['macrostrat_xdd_schema_name']
app, db = load_flask_app(SCHEMA_NAME)
CORS(app)
re_processor = REProcessor("id_maps")

def get_complete_table_name(table_name):
    return SCHEMA_NAME + "." + table_name

def verify_key_presents(input, required_keys):
    for key in required_keys:
        if key not in input:
            return "Request missing field " + key
    
    return None

def get_model_metadata(request_data, additional_data):
    # Verify that we have the required metada
    verify_result = verify_key_presents(request_data, ["model_name", "model_version"])
    if verify_result is not None:
        return False, verify_result

    # Verify that the model already exists and gets its internal model id
    models_table_name = get_complete_table_name("model")
    models_table = db.metadata.tables[models_table_name]
    model_name = request_data["model_name"]
    try:
        # Execute the select query
        models_select_statement = SELECT_STATEMENT(models_table)
        models_select_statement = models_select_statement.where(models_table.c.name == model_name)
        models_result = db.session.execute(models_select_statement).all()

        # Ensure we got a result
        if len(models_result) == 0:
            return False, "Failed to find model " + model_name + " in table " + models_table_name

        # Extract the sources id
        first_row = models_result[0]._mapping
        additional_data["internal_model_id"] = str(first_row["id"])
    except:
        error_msg =  "Failed to get id for model " + model_name + " from table " + models_table_name + " due to error: " + traceback.format_exc()
        return False, error_msg
    
    # Try to insert the model version into the the table
    model_version = request_data["model_version"]
    versions_table_name = get_complete_table_name("model_version")
    versions_table = db.metadata.tables[versions_table_name]
    try:
        # Try to insert the model version
        insert_request_values = {
            "model_id" : additional_data["internal_model_id"],
            "name" : model_version
        }
        version_insert_statement = INSERT_STATEMENT(versions_table).values(**insert_request_values)
        version_insert_statement = version_insert_statement.on_conflict_do_nothing(index_elements = list(insert_request_values.keys()))
        db.session.execute(version_insert_statement)
        db.session.commit()
    except:
        error_msg =  "Failed to insert version " + model_version + " for model " + model_name + " into table " + versions_table_name + " due to error: " + traceback.format_exc()
        return False, error_msg
    
    # Get the version id for this model and version
    try:
        # Execute the select query
        version_select_statement = SELECT_STATEMENT(versions_table)
        version_select_statement = version_select_statement.where(versions_table.c.model_id == additional_data["internal_model_id"])
        version_select_statement = version_select_statement.where(versions_table.c.name == model_version)
        version_result = db.session.execute(version_select_statement).all()

        # Ensure we got a result
        if len(version_result) == 0:
            return False, "Failed to find model " + model_name + " and version " + model_version + " in table " + versions_table_name

        # Extract the sources id
        first_row = version_result[0]._mapping
        additional_data["internal_version_id"] = str(first_row["id"])
    except:
        error_msg =  "Failed to get id for version " + model_version + " for model " + model_name + " in table " + versions_table_name + " due to error: " + traceback.format_exc()
        return False, error_msg

    return True, None

def construct_bibjson(ref):
    """Construct a bibjson object from a reference object
    Note: we haven't rigorously checked this against the spec yet.
    But it's fine for now...
    """
    assert ref["success"]["hits"] == 1
    data = ref["success"]["data"][0]
    data["xdd_id"] = data.pop("_gddid")
    data.pop("abstract", None)
    return data


def find_doi(bibjson):
    if "identifier" not in bibjson:
        return None
    for ident in bibjson["identifier"]:
        if ident["type"] == "doi":
            return ident["id"]
    return None


def find_link(bibjson):
    if "link" not in bibjson:
        return None
    for link in bibjson["link"]:
        if link["type"] == "publisher":
            return link["url"]
    return None

PUBLICATIONS_URL = "https://xdd.wisc.edu/api/articles"
def record_publication(source_text, request_additional_data):
    # See if already have a result for this publication
    paper_id = source_text["paper_id"]
    publication_table_name = get_complete_table_name("publication")
    publications_table = db.metadata.tables[publication_table_name]
    found_existing_publication = False
    try:
        publication_select_statement = SELECT_STATEMENT(publications_table)
        publication_select_statement = publication_select_statement.where(publications_table.c.paper_id == paper_id)
        publication_result = db.session.execute(publication_select_statement).all()
        found_existing_publication = len(publication_result) > 0
    except:
        return False, "Failed to check for paper " + paper_id + " in table " + publication_table_name + " due to error: " + traceback.format_exc()
    
    if found_existing_publication:
        return True, None

    # If not generate the citation and insert the publication
    try:
        # Populate the insert request values
        insert_request_values = { "paper_id" : paper_id}

        # Make the request and get the result
        result = requests.get(PUBLICATIONS_URL, params = {"docid" : paper_id})
        data = result.json()

        # Add in the bibjson
        citation_json = construct_bibjson(data)
        insert_request_values["citation"] = citation_json

        # Add in the doi and url
        doi = find_doi(citation_json)
        if doi is not None:
            insert_request_values["doi"] = doi

        url = find_link(citation_json)
        if url is not None:
            insert_request_values["url"] = url
        
        # Make the insert request
        publication_insert_request = INSERT_STATEMENT(publications_table).values(**insert_request_values)
        result = db.session.execute(publication_insert_request)
        db.session.commit()
    except:
        return False, "Failed to insert publication for paper " + paper_id + " into table " + publication_table_name + " due to error: " + traceback.format_exc() 

def record_source_text(source_text, request_additional_data):
    # Verify that we have the required fields
    required_source_fields = ["preprocessor_id", "paper_id", "hashed_text", "weaviate_id", "paragraph_text"]
    source_verify_result = verify_key_presents(source_text, required_source_fields)
    if source_verify_result is not None:
        return False, source_verify_result
    
    # First record the publication
    sucess, error_msg = record_publication(source_text, request_additional_data)
    if not sucess:
        return sucess, error_msg

    # Try to record this source
    paragraph_weaviate_id = source_text["weaviate_id"]
    internal_run_id = request_additional_data["internal_run_id"]
    sources_table_name = get_complete_table_name("source_text")
    sources_table = db.metadata.tables[sources_table_name]
    try:
        # Get the sources values
        sources_values = {}
        for key_name in required_source_fields:
            sources_values[key_name] = source_text[key_name]
        sources_values["model_run_id"] = internal_run_id
        
        sources_insert_statement = INSERT_STATEMENT(sources_table).values(**sources_values)
        sources_insert_statement = sources_insert_statement.on_conflict_do_nothing(index_elements = ["model_run_id", "weaviate_id"])
        db.session.execute(sources_insert_statement)
        db.session.commit()
    except:
        return False, "Failed to insert paragraph with weaviate id " + source_text["weaviate_id"] + " into table " + sources_table_name + " due to error: " + traceback.format_exc()

    # Then try to get the internal source id
    try:
        source_id_select_statement = SELECT_STATEMENT(sources_table.c.id)
        source_id_select_statement = source_id_select_statement.where(sources_table.c.model_run_id == internal_run_id)
        source_id_select_statement = source_id_select_statement.where(sources_table.c.weaviate_id == paragraph_weaviate_id)
        source_id_result = db.session.execute(source_id_select_statement).all()

        # Ensure we got a result
        if len(source_id_result) == 0:
            return False, "Found zero rows in " + sources_table_name + " table having run id of " + internal_run_id + " and weaviate para " + paragraph_weaviate_id
        first_row = source_id_result[0]._mapping
        request_additional_data["internal_source_id"] = first_row["id"]
    except:
        return False, "Failed to find internal source id for run " +  internal_run_id + " and weavite paragraph " + paragraph_weaviate_id + " due to error: " + traceback.format_exc()

    return True, None

def get_lith_id(lithology):
    try:
        result = requests.get("https://macrostrat.org/api/v2/defs/lithologies", params = {"lith" : lithology.lower()})
        result_data = result.json()

        # Ensure the request has a result
        if "success" not in result_data or "data" not in result_data["success"] or len(result_data["success"]["data"]) == 0:
            return True, None

        # Extract that result
        first_result = result_data["success"]["data"][0]
        return True, first_result["lith_id"]
    except:
        return False, "Failed to get id for lith " + lithology + " due to error: " + traceback.format_exc()

def get_lith_att_id(lith_attribute):
    try:
        result = requests.get("https://macrostrat.org/api/v2/defs/lithology_attributes", params = {"lith_att" : lith_attribute.lower()})
        result_data = result.json()

        # Ensure the request has a result
        if "success" not in result_data or "data" not in result_data["success"] or len(result_data["success"]["data"]) == 0:
            return True, None

        # Extract that result
        first_result = result_data["success"]["data"][0]
        return True, first_result["lith_att_id"]
    except:
        return False, "Failed to get id for lith attribute " + lith_attribute + " due to error: " + traceback.format_exc()

def get_strat_id(strat_name):
    try:
        result = requests.get("https://macrostrat.org/api/v2/defs/strat_names", params = {"strat_name_like" : strat_name.lower()})
        result_data = result.json()

        # Ensure the request has a result
        if "success" not in result_data or "data" not in result_data["success"] or len(result_data["success"]["data"]) == 0:
            return True, None

        # Extract that result
        first_result = result_data["success"]["data"][0]
        return True, first_result["strat_name_id"]
    except:
        return False, "Failed to get id for strat name " + strat_name + " due to error: " + traceback.format_exc()

ENTITY_TYPE_MAPPING = {
    "lith" : ("lith_id", get_lith_id),
    "lith_att" : ("lith_att_id", get_lith_att_id),
    "strat_name" : ("strat_name_id", get_strat_id)
}

def get_entity_type_id(entity_type):
    entity_type_table_name = get_complete_table_name("entity_type")
    entity_type_table = db.metadata.tables[entity_type_table_name]

    # First try to get the entity type
    try:
        entity_type_id_select_statement = SELECT_STATEMENT(entity_type_table)
        entity_type_id_select_statement = entity_type_id_select_statement.where(entity_type_table.c.name == entity_type)
        entity_type_result = db.session.execute(entity_type_id_select_statement).all()

        # Ensure we got a result
        if len(entity_type_result) > 0:
            first_row = entity_type_result[0]._mapping
            return True, first_row["id"]
    except:
        return False, "Failed to find entity type " + entity_type + " in table " + entity_type_table_name + " due to error: " + traceback.format_exc()

    # Try to insert the entity type
    try:
        entity_type_insert_values = {
            "name" : entity_type,
        }
        entity_type_insert_statement = INSERT_STATEMENT(entity_type_table).values(**entity_type_insert_values)
        entity_type_insert_statement = entity_type_insert_statement.on_conflict_do_nothing(index_elements = list(entity_type_insert_values.keys()))
        result = db.session.execute(entity_type_insert_statement)
        db.session.commit()

        # Get the internal id
        result_insert_keys = result.inserted_primary_key
        if len(result_insert_keys) == 0:
            return False, "Insert statement " + str(entity_type_insert_statement) + " returned zero primary keys"

        return True, result_insert_keys[0]
    except:
        return False, "Failed to insert entity type " + entity_type + " into table " + entity_type_table_name + " due to error: " + traceback.format_exc()

def get_entity_id(entity_name, entity_type, request_additional_data):
    # Record the entity type
    success, entity_type_id = get_entity_type_id(entity_type)
    if not success:
        return success, entity_type_id
    
    # Determine the values to write to the entities table
    entity_insert_request_values = {
        "name" : entity_name,
        "entity_type_id" : entity_type_id,
        "model_run_id" : request_additional_data["internal_run_id"],
        "source_id" : request_additional_data["internal_source_id"]
    }

    # Add in the start and end indexs
    lower_para_text = request_additional_data["paragraph_txt"].lower()
    lower_name = entity_name.lower()
    if lower_name in lower_para_text:
        start_idx = lower_para_text.index(lower_name)
        entity_insert_request_values["start_index"] = start_idx
        entity_insert_request_values["end_index"] = start_idx + len(lower_name)
    
    # See if we can link to a macrostrat id
    if entity_type in ENTITY_TYPE_MAPPING:
        key_name, id_getter = ENTITY_TYPE_MAPPING[entity_type]
        sucess, id_val = id_getter(entity_name)

        # First ensure the request sucessed
        if not sucess:
            return success, id_val

        # Else ensure we can record the value
        if id_val is not None:
            entity_insert_request_values[key_name] = id_val
    
    # Insert in the result into the table
    entity_table_name = get_complete_table_name("entity")
    entity_table = db.metadata.tables[entity_table_name]
    try:
        # Execute the request
        entity_insert_request = INSERT_STATEMENT(entity_table).values(**entity_insert_request_values)
        entity_insert_request = entity_insert_request.on_conflict_do_update(constraint='entity_unique', set_ = entity_insert_request_values)
        result = db.session.execute(entity_insert_request)
        db.session.commit()

        # Get the internal id
        result_insert_keys = result.inserted_primary_key
        if len(result_insert_keys) == 0:
            return False, "Insert statement " + str(entity_insert_request) + " returned zero primary keys"

        return True, result_insert_keys[0]
    except:
        return False, "Failed to entity " + entity_name + " of type " + entity_type + " into table " + entity_table_name + " due to error:" + traceback.format_exc()

def record_single_entity(entity, request_additional_data):
    # Ensure that we have the required metadata fields
    entity_required_fields = verify_key_presents(entity, ["entity", "entity_type"])
    if entity_required_fields is not None:
        return False, entity_required_fields

    return get_entity_id(entity["entity"], entity["entity_type"], request_additional_data)

def get_relationship_type_id(relationship_type):
    relationship_type_table_name = get_complete_table_name("relationship_type")
    relationship_type_table = db.metadata.tables[relationship_type_table_name]

    # First try to get the relationship type 
    try:
        relationship_type_id_select_statement = SELECT_STATEMENT(relationship_type_table)
        relationship_type_id_select_statement = relationship_type_id_select_statement.where(relationship_type_table.c.name == relationship_type)
        relationship_type_result = db.session.execute(relationship_type_id_select_statement).all()

        # Ensure we got a result
        if len(relationship_type_result) > 0:
            first_row = relationship_type_result[0]._mapping
            return True, first_row["id"]
    except:
        return False, "Failed to find entity type " + relationship_type + " in table " + relationship_type_table_name + " due to error: " + traceback.format_exc()

    # If not try to insert the relationship type into the table
    try:
        relationship_type_insert_values = {
            "name" : relationship_type,
        }
        relationship_type_insert_statement = INSERT_STATEMENT(relationship_type_table).values(**relationship_type_insert_values)
        relationship_type_insert_statement = relationship_type_insert_statement.on_conflict_do_nothing(index_elements = list(relationship_type_insert_values.keys()))
        result = db.session.execute(relationship_type_insert_statement)
        db.session.commit()

        # Get the internal id
        result_insert_keys = result.inserted_primary_key
        if len(result_insert_keys) == 0:
            return False, "Insert statement " + str(relationship_type_insert_statement) + " returned zero primary keys"

        return True, result_insert_keys[0]
    except:
        return False, "Failed to insert entity type " + relationship_type + " into table " + relationship_type_table_name + " due to error: " + traceback.format_exc()

RELATIONSHIP_DETAILS = {
    "strat" : ("strat_to_lith", "strat_name", "lith"),
    "att" : ("lith_to_attribute", "lith", "lith_att")
}
def record_relationship(relationship, request_additional_data):
    # Ensure that we have the required metadata fields
    relationship_required_fields = verify_key_presents(relationship, ["src", "relationship_type", "dst"])
    if relationship_required_fields is not None:
        return False, relationship_required_fields
    
    # Extract the types
    provided_relationship_type = relationship["relationship_type"]
    db_relationship_type, src_entity_type, dst_entity_type = None, None, None
    for key_name in RELATIONSHIP_DETAILS:
        if provided_relationship_type.startswith(key_name):
            db_relationship_type, src_entity_type, dst_entity_type = RELATIONSHIP_DETAILS[key_name]
            break
    
    # Ignore this type
    if db_relationship_type is None or src_entity_type is None or dst_entity_type is None:
        return True, ""
    
    # Record the relationship type
    success, relationship_type_id = get_relationship_type_id(db_relationship_type)
    if not success:
        return success, relationship_type_id
    
    # Get the entity ids
    success, src_entity_id = get_entity_id(relationship["src"], src_entity_type, request_additional_data)
    if not success:
        return success, src_entity_id

    success, dst_entity_id = get_entity_id(relationship["dst"], dst_entity_type, request_additional_data)
    if not success:
        return success, dst_entity_id

    # Now record the relationship
    relationship_table_name = get_complete_table_name("relationship")
    relationship_table = db.metadata.tables[relationship_table_name]
    try:
        # Get the sources values
        relationship_insert_values = {
            "relationship_type_id" : relationship_type_id,
            "model_run_id" : request_additional_data["internal_run_id"],
            "source_id" : request_additional_data["internal_source_id"],
            "src_entity_id" : src_entity_id,
            "dst_entity_id" : dst_entity_id
        }
        
        relationship_insert_statement = INSERT_STATEMENT(relationship_table).values(**relationship_insert_values)
        relationship_insert_statement = relationship_insert_statement.on_conflict_do_nothing(index_elements = ["model_run_id", "src_entity_id", "dst_entity_id", "source_id"])
        db.session.execute(relationship_insert_statement)
        db.session.commit()
    except:
        return False, "Failed to insert relationship with src " + relationship["src"] + " and dst " + relationship["dst"] + " into table " + relationship_table_name + " due to error: " + traceback.format_exc()
    
    return True, None

def record_for_result(single_result, request_additional_data):
    # Ensure that we have the required metadata fields
    result_required_fields = verify_key_presents(single_result, ["text"])
    if result_required_fields is not None:
        return False, result_required_fields
    
    # Record the source text
    sucess, error_msg = record_source_text(single_result["text"], request_additional_data)
    if not sucess:
        return sucess, error_msg
    
    # Record the relationships
    request_additional_data["paragraph_txt"] = single_result["text"]["paragraph_text"]
    if "relationships" in single_result:
        for relationship in single_result["relationships"]:
            sucessful, message = record_relationship(relationship, request_additional_data)
            if not sucessful:
                return sucessful, message
    
    # Record just the entities
    if "just_entities" in single_result:
        for entity in single_result["just_entities"]:
            sucessful, err_msg = record_single_entity(entity, request_additional_data)
            if not sucessful:
                return sucessful, err_msg
    
    return True, None

def process_input_request(request_data):
    # Ensure that we have the required metadata fields
    run_verify_result = verify_key_presents(request_data, ["run_id", "results"])
    if run_verify_result is not None:
        return False, run_verify_result

    # Get the model metadata for this model
    request_additional_data = {}
    sucess, err_msg = get_model_metadata(request_data, request_additional_data)
    if not sucess:
        return sucess, err_msg

    # Record each result as an independent run
    extraction_pipeline_id = request_data["extraction_pipeline_id"]
    all_results = request_data["results"]
    base_model_run_id = request_data["run_id"]
    model_run_table_name = get_complete_table_name("model_run")
    model_run_table = db.metadata.tables[model_run_table_name]

    for idx, current_result in enumerate(all_results):
        # First record this result as an independent run in the models table
        model_run_id = str(base_model_run_id) + "_" + str(idx)
        try:
            model_run_insert_values = {
                "run_id" : model_run_id,
                "model_id" : request_additional_data["internal_model_id"],
                "version_id" : request_additional_data["internal_version_id"],
                "extraction_pipeline_id" : extraction_pipeline_id
            }
            model_run_insert_request = INSERT_STATEMENT(model_run_table).values(**model_run_insert_values)
            result = db.session.execute(model_run_insert_request)
            db.session.commit()
        except Exception:
            return False, "Failed to insert run for base id " + str(base_model_run_id) + " and idx " + str(idx) + " due to error: " + traceback.format_exc()
        
        # Now get the internal run id
        try:
            internal_id_select_statement = SELECT_STATEMENT(model_run_table.c.id)
            internal_id_select_statement = internal_id_select_statement.where(model_run_table.c.run_id == model_run_id)
            internal_id_select_statement = internal_id_select_statement.where(model_run_table.c.extraction_pipeline_id == extraction_pipeline_id)
            internal_id_select_statement = internal_id_select_statement.where(model_run_table.c.model_id == request_additional_data["internal_model_id"])
            internal_id_select_statement = internal_id_select_statement.where(model_run_table.c.version_id == request_additional_data["internal_version_id"])
            internal_id_result = db.session.execute(internal_id_select_statement).all()

            # Ensure we got a result
            if len(internal_id_result) == 0:
                return False, "Found zero rows in the " + model_run_table_name + " table having run id of " + model_run_id
            first_row = internal_id_result[0]._mapping
            request_additional_data["internal_run_id"] = first_row["id"]
        except:
            return False, "Failed to find internal run id for base id " + str(base_model_run_id) + " and idx " + str(idx) + " due to error: " + traceback.format_exc()

        # Now try to record the result
        sucess, error_msg = record_for_result(current_result, request_additional_data)
        if not sucess:
            return sucess, error_msg

    return True, None

@app.route("/record_run", methods=["POST"])
def record_run():
    # Record the run
    request_data = request.get_json()
    print("Got request", request_data)
    sucessful, error_msg = process_input_request(request_data)
    if not sucessful:
        print("Returning error of", error_msg)
        return jsonify({"error" : error_msg}), 400
    return jsonify({"sucess" : "Sucessfully processed the run"}), 200


@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint"""

    return jsonify({"status": "Server Running"}), 200


if __name__ == "__main__":
   app.run(host = "0.0.0.0", port = 9543, debug = True)