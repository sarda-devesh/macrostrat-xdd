
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic_settings import BaseSettings
from sqlalchemy.dialects.postgresql import insert as INSERT_STATEMENT
from sqlalchemy import select as SELECT_STATEMENT, text
from sqlalchemy.orm import Session
from fuzzysearch import find_near_matches
import traceback
import hashlib
import requests

from macrostrat_db_insertion.database import connect_engine, dispose_engine, get_base, get_session
from macrostrat_db_insertion.security import has_access


class Settings(BaseSettings):
    uri: str
    SCHEMA: str
    max_tries: int = 5


settings = Settings()


@asynccontextmanager
async def setup_engine(a: FastAPI):
    """Return database client instance."""
    connect_engine(settings.uri, settings.SCHEMA)
    yield
    dispose_engine()

app = FastAPI(
    lifespan=setup_engine
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_complete_table_name(table_name):
    return settings.SCHEMA + "." + table_name

def verify_key_presents(input, required_keys):
    for key in required_keys:
        if key not in input:
            return "Request missing field " + key

    return None

def get_model_metadata(request_data, additional_data, session: Session):
    # Verify that we have the required metada
    verify_result = verify_key_presents(request_data, ["model_name", "model_version"])
    if verify_result is not None:
        return False, verify_result

    # Verify that the model already exists and gets its internal model id
    models_table_name = get_complete_table_name("model")
    models_table = get_base().metadata.tables[models_table_name]
    model_name = request_data["model_name"]
    try:
        # Execute the select query
        models_select_statement = SELECT_STATEMENT(models_table)
        models_select_statement = models_select_statement.where(models_table.c.name == model_name)
        models_result = session.execute(models_select_statement).all()

        # Ensure we got a result and if so get the model id
        if len(models_result) > 0:
            first_row = models_result[0]._mapping
            additional_data["internal_model_id"] = first_row["id"]
    except:
        error_msg =  "Failed to get id for model " + model_name + " from table " + models_table_name + " due to error: " + traceback.format_exc()
        return False, error_msg

    # If not then insert the model and get its id
    if "internal_model_id" not in additional_data:
        try:
            model_insert_values = {
                "name" : model_name,
            }
            model_insert_statement = INSERT_STATEMENT(models_table).values(**model_insert_values)
            model_insert_statement = model_insert_statement.on_conflict_do_nothing(index_elements = list(model_insert_values.keys()))
            result = session.execute(model_insert_statement)
            session.commit()

            # Get the internal id
            result_insert_keys = result.inserted_primary_key
            if len(result_insert_keys) == 0:
                return False, "Insert statement " + str(model_insert_statement) + " returned zero primary keys"

            additional_data["internal_model_id"] = result_insert_keys[0]
        except:
            return False, "Failed to insert model " + model_name + " into table " + models_table_name + " due to error: " + traceback.format_exc()

    # Try to insert the model version into the the table
    model_version = str(request_data["model_version"])
    versions_table_name = get_complete_table_name("model_version")
    versions_table = get_base().metadata.tables[versions_table_name]
    try:
        # Try to insert the model version
        insert_request_values = {
            "model_id" : additional_data["internal_model_id"],
            "name" : model_version
        }
        version_insert_statement = INSERT_STATEMENT(versions_table).values(**insert_request_values)
        version_insert_statement = version_insert_statement.on_conflict_do_nothing(index_elements = list(insert_request_values.keys()))
        session.execute(version_insert_statement)
        session.commit()
    except:
        error_msg =  "Failed to insert version " + str(model_version) + " for model " + model_name + " into table " + versions_table_name + " due to error: " + traceback.format_exc()
        return False, error_msg

    # Get the version id for this model and version
    try:
        # Execute the select query
        version_select_statement = SELECT_STATEMENT(versions_table)
        version_select_statement = version_select_statement.where(versions_table.c.model_id == additional_data["internal_model_id"])
        version_select_statement = version_select_statement.where(versions_table.c.name == model_version)
        version_result = session.execute(version_select_statement).all()

        # Ensure we got a result
        if len(version_result) == 0:
            return False, "Failed to find model " + model_name + " and version " + model_version + " in table " + versions_table_name

        # Extract the sources id
        first_row = version_result[0]._mapping
        additional_data["internal_version_id"] = str(first_row["id"])
    except:
        error_msg =  "Failed to get id for version " + str(model_version) + " for model " + model_name + " in table " + versions_table_name + " due to error: " + traceback.format_exc()
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
def record_publication(source_text, request_additional_data, session: Session):
    # See if already have a result for this publication
    paper_id = source_text["paper_id"]
    publication_table_name = get_complete_table_name("publication")
    publications_table = get_base().metadata.tables[publication_table_name]
    found_existing_publication = False
    try:
        publication_select_statement = SELECT_STATEMENT(publications_table)
        publication_select_statement = publication_select_statement.where(publications_table.c.paper_id == paper_id)
        publication_result = session.execute(publication_select_statement).all()
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
        result = session.execute(publication_insert_request)
        session.commit()
    except:
        return False, "Failed to insert publication for paper " + paper_id + " into table " + publication_table_name + " due to error: " + traceback.format_exc()

    return True, None

def get_weaviate_text_id(source_text, request_additional_data, session: Session):
    # Verify that we have the required fields
    required_source_fields = ["preprocessor_id", "paper_id", "hashed_text", "weaviate_id", "paragraph_text"]
    source_verify_result = verify_key_presents(source_text, required_source_fields)
    if source_verify_result is not None:
        return False, source_verify_result

    # First record the publication
    sucess, error_msg = record_publication(source_text, request_additional_data, session)
    if not sucess:
        return sucess, error_msg

    # Try to record this source
    curr_text_type = source_text["text_type"]
    paragraph_hash = source_text["hashed_text"]
    sources_table_name = get_complete_table_name("source_text")
    sources_table = get_base().metadata.tables[sources_table_name]
    try:
        # Get the sources values
        sources_values = {}
        for key_name in required_source_fields:
            sources_values[key_name] = source_text[key_name]
        sources_values["source_text_type"] = curr_text_type

        sources_insert_statement = INSERT_STATEMENT(sources_table).values(**sources_values)
        sources_insert_statement = sources_insert_statement.on_conflict_do_nothing(index_elements = ["source_text_type", "paragraph_text"])
        session.execute(sources_insert_statement)
        session.commit()
    except:
        return False, "Failed to insert paragraph with weaviate id " + source_text["weaviate_id"] + " into table " + sources_table_name + " due to error: " + traceback.format_exc()

    # Then try to get the internal source id
    try:
        source_id_select_statement = SELECT_STATEMENT(sources_table.c.id)
        source_id_select_statement = source_id_select_statement.where(sources_table.c.source_text_type == curr_text_type)
        source_id_select_statement = source_id_select_statement.where(sources_table.c.paragraph_text == source_text["paragraph_text"])
        source_id_result = session.execute(source_id_select_statement).all()

        # Ensure we got a result
        if len(source_id_result) == 0:
            return False, "Found zero rows in " + sources_table_name + " table for weaviate paragraph having hash " + paragraph_hash
        first_row = source_id_result[0]._mapping
        return True, first_row["id"]
    except:
        return False, "Failed to find internal source id for weaviate paragraph having hash " + paragraph_hash + " due to error: " + traceback.format_exc()

def get_map_description_id(source_text, request_additional_data, session: Session):
    # Verify that we have the required fields
    required_source_fields = ["paragraph_text", "legend_id"]
    source_verify_result = verify_key_presents(source_text, required_source_fields)
    if source_verify_result is not None:
        return False, source_verify_result

    # Try to record this source
    paragraph_text = str(source_text["paragraph_text"])
    text_hash = str(hashlib.sha256(paragraph_text.encode("ascii")).hexdigest())
    curr_text_type = source_text["text_type"]
    sources_table_name = get_complete_table_name("source_text")
    sources_table = get_base().metadata.tables[sources_table_name]
    legend_id = source_text["legend_id"]
    try:
        # Get the sources values
        sources_values = {
            "source_text_type": curr_text_type,
            "paragraph_text" : source_text["paragraph_text"],
            "map_legend_id" : legend_id,
            "hashed_text" : text_hash
        }

        sources_insert_statement = INSERT_STATEMENT(sources_table).values(**sources_values)
        sources_insert_statement = sources_insert_statement.on_conflict_do_nothing(index_elements = ["source_text_type", "paragraph_text"])
        session.execute(sources_insert_statement)
        session.commit()
    except:
        return False, "Failed to insert paragraph with legend id " + str(legend_id) + " into table " + sources_table_name + " due to error: " + traceback.format_exc()

    # Then try to get the internal source id
    try:
        source_id_select_statement = SELECT_STATEMENT(sources_table.c.id)
        source_id_select_statement = source_id_select_statement.where(sources_table.c.source_text_type == curr_text_type)
        source_id_select_statement = source_id_select_statement.where(sources_table.c.paragraph_text == source_text["paragraph_text"])
        source_id_result = session.execute(source_id_select_statement).all()

        # Ensure we got a result
        if len(source_id_result) == 0:
            return False, "Found zero rows in " + sources_table_name + " table for map legend id " + legend_id
        first_row = source_id_result[0]._mapping
        return True, first_row["id"]
    except:
        return False, "Failed to find internal source id for map legend having id " + legend_id + " due to error: " + traceback.format_exc()

METHOD_TO_PROCESS_TEXT = {
    "weaviate_text" : get_weaviate_text_id,
    "map_descriptions" : get_map_description_id
}
def get_source_text_id(source_text, request_additional_data, session: Session):
    # Ensure that we have the required metadata fields
    text_required_text = verify_key_presents(source_text, ["text_type", "paragraph_text"])
    if text_required_text is not None:
        return False, text_required_text

    # Determine the method to use based on the text type
    request_additional_data["paragraph_txt"] = source_text["paragraph_text"]
    text_type = source_text["text_type"]
    if text_type not in METHOD_TO_PROCESS_TEXT:
        return False, "Server currently doesn't support text of type " + text_type

    return METHOD_TO_PROCESS_TEXT[text_type](source_text, request_additional_data, session)

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

def get_entity_type_id(entity_type, session: Session):
    entity_type_table_name = get_complete_table_name("entity_type")
    entity_type_table = get_base().metadata.tables[entity_type_table_name]

    # First try to get the entity type
    try:
        entity_type_id_select_statement = SELECT_STATEMENT(entity_type_table)
        entity_type_id_select_statement = entity_type_id_select_statement.where(entity_type_table.c.name == entity_type)
        entity_type_result = session.execute(entity_type_id_select_statement).all()

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
        result = session.execute(entity_type_insert_statement)
        session.commit()

        # Get the internal id
        result_insert_keys = result.inserted_primary_key
        if len(result_insert_keys) == 0:
            return False, "Insert statement " + str(entity_type_insert_statement) + " returned zero primary keys"

        return True, result_insert_keys[0]
    except:
        return False, "Failed to insert entity type " + entity_type + " into table " + entity_type_table_name + " due to error: " + traceback.format_exc()

def get_entity_id(entity_name, entity_type, request_additional_data, session: Session, provided_start_idx = None, provided_end_idx = None):
    # Record the entity type
    success, entity_type_id = get_entity_type_id(entity_type, session)
    if not success:
        return success, entity_type_id

    # Determine the values to write to the entities table
    entity_insert_request_values = {
        "name" : entity_name,
        "entity_type_id" : entity_type_id,
        "model_run_id" : request_additional_data["internal_run_id"],
    }

    entity_start_idx, entity_end_idx, str_match_type = provided_start_idx, provided_end_idx, "provided"
    if entity_start_idx is None or entity_end_idx is None:
        # See if we can get a direct match
        lower_para_text = request_additional_data["paragraph_txt"].lower()
        lower_name = entity_name.lower()
        if lower_name in lower_para_text:
            entity_start_idx = lower_para_text.index(lower_name)
            entity_end_idx = entity_start_idx + len(lower_name)
            str_match_type = "exact"
        else:
            curr_max_l_dist = max(int(0.1 * len(lower_name)), 2)
            start_idx, end_idx = -1, -1
            while start_idx < 0:
                matches = find_near_matches(lower_name, lower_para_text, max_l_dist = curr_max_l_dist)
                if len(matches) > 0:
                    # Find the match with the least distance
                    best_match_idx = 0
                    for idx in range(1, len(matches)):
                        if matches[idx].dist < matches[best_match_idx].dist:
                            best_match_idx = idx

                    # Record the idx
                    start_idx, end_idx = matches[best_match_idx].start,  matches[best_match_idx].end

                curr_max_l_dist *= 2

            # Record the results
            entity_start_idx, entity_end_idx, str_match_type = start_idx, end_idx, "fuzzy"

    entity_insert_request_values["start_index"] = entity_start_idx
    entity_insert_request_values["end_index"] = entity_end_idx
    entity_insert_request_values["str_match_type"] = str_match_type

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
    entity_table = get_base().metadata.tables[entity_table_name]
    try:
        # Execute the request
        entity_insert_request = INSERT_STATEMENT(entity_table).values(**entity_insert_request_values)
        entity_insert_request = entity_insert_request.on_conflict_do_nothing(index_elements = ["name", "model_run_id", "entity_type_id", "start_index", "end_index"])
        result = session.execute(entity_insert_request)
        session.commit()
    except:
        return False, "Failed to entity " + entity_name + " of type " + entity_type + " into table " + entity_table_name + " due to error:" + traceback.format_exc()

    # Get the entity id for the inserted value
    try:
        entity_select_statement = SELECT_STATEMENT(entity_table.c.id)
        entity_select_statement = entity_select_statement.where(entity_table.c.name == entity_insert_request_values["name"])
        entity_select_statement = entity_select_statement.where(entity_table.c.model_run_id == entity_insert_request_values["model_run_id"])
        entity_select_statement = entity_select_statement.where(entity_table.c.entity_type_id == entity_insert_request_values["entity_type_id"])
        entity_select_statement = entity_select_statement.where(entity_table.c.start_index == entity_insert_request_values["start_index"])
        entity_select_statement = entity_select_statement.where(entity_table.c.end_index == entity_insert_request_values["end_index"])
        entity_id_result = session.execute(entity_select_statement).all()

        # Ensure we got a result
        if len(entity_id_result) == 0:
            return False, "Found zero rows in " + entity_table_name + " table for entity " + entity_name
        first_row = entity_id_result[0]._mapping
        return True, first_row["id"]
    except:
        return False, "Failed to find internal entity id for entity " + entity_name + " due to error: " + traceback.format_exc()

def extract_indicies(request_values, expected_prefix):
    provided_start_idx, provided_end_idx = None, None
    start_search_term, end_search_term = expected_prefix + "start_idx", expected_prefix + "end_idx"

    if start_search_term in request_values:
        try:
            provided_start_idx = int(request_values[start_search_term])
        except:
            return False, "Failed to parse " + request_values[start_search_term] + " as an integer due an error " + traceback.format_exc()

        # If the start idx is provided then end idx must also be provided
        if end_search_term in request_values:
            try:
                # Also verify that end idx >= start_idx
                provided_end_idx = int(request_values[end_search_term])
                if provided_start_idx > provided_end_idx:
                    return False, "Start idx of " + str(provided_start_idx) + " is greater than end idx of " + str(provided_end_idx) + " for entity " + request_values["entity"]
            except:
                return False, "Failed to parse " + request_values[end_search_term] + " as an integer due an error " + traceback.format_exc()
        else:
            return False, f'Provided {start_search_term} but not {end_search_term} for entity ' + request_values["entity"]

    return True, (provided_start_idx, provided_end_idx)

def record_single_entity(entity, request_additional_data, session: Session):
    # Ensure that we have the required metadata fields
    entity_required_fields = verify_key_presents(entity, ["entity", "entity_type"])
    if entity_required_fields is not None:
        return False, entity_required_fields

    # See if the range is provided
    success, indicies_results = extract_indicies(entity, "")
    if not success:
        return success, indicies_results

    provided_start_idx, provided_end_idx = indicies_results
    return get_entity_id(entity["entity"], entity["entity_type"], request_additional_data, session, provided_start_idx, provided_end_idx)

def get_relationship_type_id(relationship_type, session: Session):
    relationship_type_table_name = get_complete_table_name("relationship_type")
    relationship_type_table = get_base().metadata.tables[relationship_type_table_name]

    # First try to get the relationship type
    try:
        relationship_type_id_select_statement = SELECT_STATEMENT(relationship_type_table)
        relationship_type_id_select_statement = relationship_type_id_select_statement.where(relationship_type_table.c.name == relationship_type)
        relationship_type_result = session.execute(relationship_type_id_select_statement).all()

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
        result = session.execute(relationship_type_insert_statement)
        session.commit()

        # Get the internal id
        result_insert_keys = result.inserted_primary_key
        if len(result_insert_keys) == 0:
            return False, "Insert statement " + str(relationship_type_insert_statement) + " returned zero primary keys"

        return True, result_insert_keys[0]
    except:
        return False, "Failed to insert entity type " + relationship_type + " into table " + relationship_type_table_name + " due to error: " + traceback.format_exc()

UNKNOWN_ENTITY_TYPE = "Unknown"
RELATIONSHIP_DETAILS = {
    "strat" : ("strat_to_lith", "strat_name", "lith"),
    "att" : ("lith_to_attribute", "lith", "lith_att")
}
def record_relationship(relationship, request_additional_data, session: Session):
    # Ensure that we have the required metadata fields
    relationship_required_fields = verify_key_presents(relationship, ["src", "relationship_type", "dst"])
    if relationship_required_fields is not None:
        return False, relationship_required_fields

    # Extract the types
    provided_relationship_type = relationship["relationship_type"]
    db_relationship_type, src_entity_type, dst_entity_type = provided_relationship_type, UNKNOWN_ENTITY_TYPE, UNKNOWN_ENTITY_TYPE
    for key_name in RELATIONSHIP_DETAILS:
        if provided_relationship_type.startswith(key_name):
            db_relationship_type, src_entity_type, dst_entity_type = RELATIONSHIP_DETAILS[key_name]
            break

    # Record the relationship type
    success, relationship_type_id = get_relationship_type_id(db_relationship_type, session)
    if not success:
        return success, relationship_type_id

    # Extract the source indicies
    success, indicies_results = extract_indicies(relationship, "src_")
    if not success:
        return success, indicies_results
    src_provided_start_idx, src_provided_end_idx = indicies_results

    # Get the src entity ids
    success, src_entity_id = get_entity_id(relationship["src"], src_entity_type, request_additional_data, session, src_provided_start_idx, src_provided_end_idx)
    if not success:
        return success, src_entity_id

    # Extract the dest indicies
    success, indicies_results = extract_indicies(relationship, "dst_")
    if not success:
        return success, indicies_results
    dst_provided_start_idx, dst_provided_end_idx = indicies_results

    success, dst_entity_id = get_entity_id(relationship["dst"], dst_entity_type, request_additional_data, session, dst_provided_start_idx, dst_provided_end_idx)
    if not success:
        return success, dst_entity_id

    # Now record the relationship
    relationship_table_name = get_complete_table_name("relationship")
    relationship_table = get_base().metadata.tables[relationship_table_name]
    try:
        # Get the sources values
        relationship_insert_values = {
            "relationship_type_id" : relationship_type_id,
            "model_run_id" : request_additional_data["internal_run_id"],
            "src_entity_id" : src_entity_id,
            "dst_entity_id" : dst_entity_id
        }

        if "reasoning" in relationship:
            relationship_insert_values["reasoning"] = relationship["reasoning"]

        relationship_insert_statement = INSERT_STATEMENT(relationship_table).values(**relationship_insert_values)
        relationship_insert_statement = relationship_insert_statement.on_conflict_do_nothing(index_elements = ["model_run_id", "src_entity_id", "dst_entity_id", "relationship_type_id"])
        session.execute(relationship_insert_statement)
        session.commit()
    except:
        return False, "Failed to insert relationship with src " + relationship["src"] + " and dst " + relationship["dst"] + " into table " + relationship_table_name + " due to error: " + traceback.format_exc()

    return True, None

def get_previous_run(source_text_id, session: Session):
    # Load the latest run table
    latest_run_table_name = get_complete_table_name("latest_run_per_text")
    latest_run_table = get_base().metadata.tables[latest_run_table_name]

    # Get the latest for the current source text
    prev_run_id = None
    try:
        previous_id_for_source_select_statement = SELECT_STATEMENT(latest_run_table)
        previous_id_for_source_select_statement = previous_id_for_source_select_statement.where(latest_run_table.c.source_text_id == source_text_id)
        previous_id_result = session.execute(previous_id_for_source_select_statement).all()

        # Ensure we got a result
        if len(previous_id_result) > 0:
            first_row = previous_id_result[0]._mapping
            prev_run_id = first_row["latest_run_id"]
    except:
        return False, "Failed to get latest run id for source text " + source_text_id + " from view " + latest_run_table_name + " due to error: " + traceback.format_exc()

    return True, prev_run_id

def process_input_request(request_data, session):
    # Ensure that we have the required metadata fields
    run_verify_result = verify_key_presents(request_data, ["run_id", "results"])
    if run_verify_result is not None:
        return False, run_verify_result

    # Get the model metadata for this model
    request_additional_data = {}
    sucess, err_msg = get_model_metadata(request_data, request_additional_data, session)
    if not sucess:
        return sucess, err_msg

    # Record each result as an independent run
    extraction_pipeline_id = request_data["extraction_pipeline_id"]
    all_results = request_data["results"]
    base_model_run_id = request_data["run_id"]
    model_run_table_name = get_complete_table_name("model_run")
    model_run_table = get_base().metadata.tables[model_run_table_name]

    for idx, current_result in enumerate(all_results):
        # Ensure that we have the required metadata fields
        result_required_fields = verify_key_presents(current_result, ["text"])
        if result_required_fields is not None:
            return False, result_required_fields

        # First get the source text id for this result
        success, source_text_id = get_source_text_id(current_result["text"], request_additional_data, session)
        if not success:
            return success, source_text_id

        # Then get the previous run for this result
        success, previous_run_id = get_previous_run(source_text_id, session)
        if not success:
            return success, previous_run_id

        # First record this result as an independent run in the models table
        model_run_id = str(base_model_run_id) + "_" + str(idx)
        try:
            model_run_insert_values = {
                "extraction_job_id" : model_run_id,
                "model_id" : request_additional_data["internal_model_id"],
                "version_id" : request_additional_data["internal_version_id"],
                "extraction_pipeline_id" : extraction_pipeline_id,
                "source_text_id" : source_text_id
            }
            if previous_run_id is not None:
                model_run_insert_values["supersedes"] = previous_run_id

            model_run_insert_request = INSERT_STATEMENT(model_run_table).values(**model_run_insert_values)
            model_run_insert_request = model_run_insert_request.on_conflict_do_update(constraint = "no_duplicate_runs", set_ = model_run_insert_values)
            result = session.execute(model_run_insert_request)
            session.commit()

            # Now get the interal run id for this new run
            result_insert_keys = result.inserted_primary_key
            if len(result_insert_keys) == 0:
                return False, "Insert statement " + str(model_run_insert_request) + " returned zero primary keys"
            request_additional_data["internal_run_id"] = result_insert_keys[0]
        except:
            return False, "Failed to insert run with extraction id " + str(base_model_run_id) + " and idx " + str(idx) + " due to error: " + traceback.format_exc()

        # Now actually record the graph
        if "relationships" in current_result:
            for relationship in current_result["relationships"]:
                sucessful, message = record_relationship(relationship, request_additional_data, session)
                if not sucessful:
                    return sucessful, message

        # Record just the entities
        if "just_entities" in current_result:
            for entity in current_result["just_entities"]:
                sucessful, err_msg = record_single_entity(entity, request_additional_data, session)
                if not sucessful:
                    return sucessful, err_msg

    return True, None

# Opentially take in user id
@app.post("/record_run")
async def record_run(
        request: Request,
        user_has_access: bool = Depends(has_access),
        session: Session = Depends(get_session)
):

    if not user_has_access:
        raise HTTPException(status_code=403, detail="User does not have access to record run")

    # Record the run
    request_data = await request.json()

    successful, error_msg = process_input_request(request_data, session)
    if not successful:
        raise HTTPException(status_code=400, detail=error_msg)

    return JSONResponse(content={"success": "Successfully processed the run"})


@app.get("/health")
async def health(
    session = Depends(get_session)
):

    health_checks = {}

    try:
        session.execute(text("SELECT 1"))
    except:
        health_checks["database"] = False
    else:
        health_checks["database"] = True

    # Test that we can get metadata
    try:
        models_table_name = get_complete_table_name("model")
        models_table = get_base().metadata.tables[models_table_name]
        models_select_statement = SELECT_STATEMENT(models_table)
        models_select_statement = models_select_statement.limit(1)
        session.execute(models_select_statement)
    except:
        health_checks["metadata"] = False
    else:
        health_checks["metadata"] = True

    return JSONResponse(content={
        "webserver": "ok",
        "healthy": health_checks
    })


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=9543)
