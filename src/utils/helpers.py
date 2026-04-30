import os
import logging
from dotenv import load_dotenv

load_dotenv()

def get_config(key):
    return os.getenv(key)

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)