# schema_infer.py
import os
from pymongo import MongoClient
from functools import lru_cache
from collections import defaultdict

def extract_paths(doc: dict, prefix: str = "") -> dict:
    """
    Recursively traverse `doc` and return a map of field_path -> set(type_names)
    """
    paths = {}
    for key, val in doc.items():
        path = f"{prefix}.{key}" if prefix else key
        typ = type(val).__name__
        paths.setdefault(path, set()).add(typ)

        # Recurse into nested dict
        if isinstance(val, dict):
            paths.update(extract_paths(val, path))

        # If list of dicts, recurse into first element
        elif isinstance(val, list) and val and isinstance(val[0], dict):
            paths.update(extract_paths(val[0], path + "[]"))

    return paths

@lru_cache()
def get_schema_map(sample_size: int = 50) -> dict:
    """
    Connects via pymongo, samples up to `sample_size` docs per collection,
    and returns a dict: { collection_name: { field_path: [type_names,...] } }
    """
    uri     = os.getenv("MONGODB_URI")
    db_name = os.getenv("DB_NAME")
    client  = MongoClient(uri)
    db      = client[db_name]

    schema_map = {}
    for coll in db.list_collection_names():
        combined = defaultdict(set)
        for doc in db[coll].find().limit(sample_size):
            for path, types in extract_paths(doc).items():
                combined[path].update(types)
        # Convert type sets to sorted lists
        schema_map[coll] = {path: sorted(list(types)) for path, types in combined.items()}

    return schema_map
