import json
from datetime import date, datetime
import logging
from pathlib import Path
from airflow.operators.python import get_current_context
logger = logging.getLogger(__name__)

def load_data():
    
    context = get_current_context()
    ds = context["ds"]
    file_date = ds.replace("-", "")
    file_path = Path("/opt/airflow/data") / f"video_data_{file_date}.json"
    try:
        logging.info(f"Processing file: video_data_{file_date}")
        
        with open(file_path, 'r', encoding='utf-8') as raw_data:
            data = json.load(raw_data)
        return data
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON from file: {file_path}")
        raise