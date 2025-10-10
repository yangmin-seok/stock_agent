import os
from dotenv import load_dotenv
from typing import Dict

# Load environment variables from .env file
load_dotenv() 

DART_API_KEY = os.getenv("DART_API_KEY")

# PostgreSQL database configuration
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}