import json
import os

def load_json_file(json_file):
    with open(json_file, 'r') as reader:
        data = json.load(reader)
    return data

class REProcessor:

    def __init__(self, ids_folder):
        self.ids_maps = {
            "lith_map" : load_json_file(os.path.join(ids_folder, "lith_id_map.json")),
            "lith_att_map" : load_json_file(os.path.join(ids_folder, "lith_att_id_map.json")),
            "strat_name_map" : load_json_file(os.path.join(ids_folder, "strat_names_map.json"))
        }
    
    def get_entity_id(self, entity_name, entity_type):
        type_id_map = self.ids_maps[entity_type + "_map"]
        entity_name = entity_name.lower()
        entity_id = -1
        if entity_name in type_id_map:
            entity_id = type_id_map[entity_name]
        
        return entity_id