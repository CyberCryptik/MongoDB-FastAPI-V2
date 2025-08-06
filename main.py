from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import APIKeyHeader
from typing import List, Dict, Any, Union  # <-- Ensure Dict, Any are imported
from bson import ObjectId
import json
import os
import logging

from schemas import AggregateRequest
from db import db
from dotenv import load_dotenv
from schema_infer import get_schema_map_and_samples

load_dotenv()
app = FastAPI()
logging.basicConfig(level=logging.INFO)

# API key setup
API_KEY = os.getenv("API_KEY")
if not API_KEY:
    raise RuntimeError("API_KEY not set in .env")

api_key_header = APIKeyHeader(name="X-API-Key")

def verify_api_key(api_key: str = Depends(api_key_header)):
    if api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

def convert_objectids(obj: Any) -> Any:
    if isinstance(obj, list):
        return [convert_objectids(v) for v in obj]
    if isinstance(obj, dict):
        return {k: convert_objectids(v) for k, v in obj.items()}
    if isinstance(obj, ObjectId):
        return str(obj)
    return obj

def normalize_objectid(stage: Any):
    if isinstance(stage, dict):
        for k, v in list(stage.items()):
            if k == "_id" and isinstance(v, str) and ObjectId.is_valid(v):
                stage[k] = ObjectId(v)
            else:
                normalize_objectid(v)
    elif isinstance(stage, list):
        for item in stage:
            normalize_objectid(item)


@app.post("/aggregate")
async def aggregate_query(
    payload: AggregateRequest,
    api_key: str = Depends(verify_api_key)
):
    raw_pipeline = payload.pipeline

    # Parse JSON strings if pipeline stages were passed as strings
    if isinstance(raw_pipeline, list) and raw_pipeline and isinstance(raw_pipeline[0], str):
        try:
            pipeline = [json.loads(stage) for stage in raw_pipeline]
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid pipeline JSON strings")
    else:
        pipeline = raw_pipeline

    if not isinstance(pipeline, list):
        raise HTTPException(status_code=400, detail="Pipeline must be a list")

    # Normalize any ObjectId strings in the pipeline
    for stage in pipeline:
        normalize_objectid(stage)

    try:
        collection = db[payload.collection]
        cursor = collection.aggregate(pipeline)
        results = await cursor.to_list(length=1000)
        # Convert ObjectId in results to JSON-friendly strings
        results = convert_objectids(results)
        return {"results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
def prune_nonjson(obj: Any) -> Any:
    """
    Recursively strip out any values that aren't plain JSON types.
    JSON-safe types: str, int, float, bool, None, list, dict.
    """
    if isinstance(obj, dict):
        result = {}
        for k, v in obj.items():
            pruned = prune_nonjson(v)
            if pruned is not None or v is None:
                result[k] = pruned
        return result
    if isinstance(obj, list):
        return [prune_nonjson(v) for v in obj]
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    # drop everything else (Binary, datetime, ObjectId, bytes, etc.)
    return None

@app.get("/schema")
def read_schema() -> Dict[str, Any]:
    """
    Public endpoint — no API key required.
    Returns both the inferred schema map and one sample doc per collection,
    with all non-JSONable fields pruned out.
    """
    try:
        data = get_schema_map_and_samples()
        pruned_samples = {
            coll: prune_nonjson(convert_objectids(doc)) if doc is not None else None
            for coll, doc in data["samples"].items()
        }
        return {
            "schema": data["schema"],
            "samples": pruned_samples
        }
    except Exception as e:
        logging.exception("Error in /schema")
        raise HTTPException(status_code=500, detail=str(e))
