import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

REDIS_CONFIG = {
    'host': os.getenv('REDIS_HOST'),
    'port': int(os.getenv('REDIS_PORT')),
    'db': int(os.getenv('REDIS_DB')),
    'password': os.getenv('REDIS_PASSWORD'),
    'socket_timeout': 5,
    'socket_connect_timeout': 5,
}

CACHE_EXPIRATION = {
    'user_data': 3600,  # 1 hour
    'extraction_results': int(os.getenv('EXTRACTION_RESULTS_CACHE_EXPIRATION')),
    'metadata': int(os.getenv('METADATA_CACHE_EXPIRATION')),
    'digital_footprint': 7200,  # 2 hours
    'source': 7200,  # 2 hours
    'personal_identity': 3600,  # 1 hour
}
