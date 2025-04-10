"""
config.py - Central configuration file for the Swiss Train Delays Analysis project

This module contains all configuration parameters, hyperparameters, and constants
used throughout the project.
"""

from datetime import datetime

# API Configuration
BASE_URL = "http://transport.opendata.ch/v1"
STATIONBOARD_ENDPOINT = f"{BASE_URL}/stationboard"
CONNECTIONS_ENDPOINT = f"{BASE_URL}/connections"
LOCATIONS_ENDPOINT = f"{BASE_URL}/locations"

# Rate Limiting Parameters
MAX_REQUESTS_PER_DAY = {
    "connections": 1000,
    "stationboard": 10080
}

# Request Counter
REQUEST_COUNTER = {
    "connections": 0,
    "stationboard": 0,
    "locations": 0,
    "last_reset": datetime.now().date()
}

# Rate Limit Tracking
RATE_LIMIT_EXCEEDED = {
    "connections": False,
    "stationboard": False,
    "locations": False,
    "reset_time": datetime.now()
}

# API Caching
API_CACHE = {}
CACHE_EXPIRY = 3600  # Cache expiry in seconds (1 hour)

# Backoff Strategy Parameters
MAX_RETRIES = 3
INITIAL_BACKOFF = 5  # seconds
MAX_BACKOFF = 60  # seconds

# File System
DEFAULT_DATA_DIR = "data"
DEFAULT_RAW_DIR = "data/raw"
DEFAULT_PROCESSED_DIR = "data/processed"
DEFAULT_ANALYSIS_DIR = "data/analysis"

# Target Stations with IDs
TARGET_STATIONS = {
    "Luzern": "8505000",
    "Zürich HB": "8503000",
    "Genève": "8501008"
}

# Connection Pairs to Analyze
CONNECTION_PAIRS = [
    ("Zürich HB", "Luzern"),
    ("Zürich HB", "Genève"),
    ("Luzern", "Genève")
]

# Data Collection Parameters
DEFAULT_TIME_SLOTS = ['06:00', '08:00', '10:00', '12:00', '14:00', '16:00', '18:00', '20:00']
DEFAULT_STATION_BOARD_LIMIT = 10
DEFAULT_CONNECTIONS_LIMIT = 10

# Analysis Parameters
DELAY_THRESHOLD_MINUTES = 0  # Trains with delays <= this value are considered "on time"