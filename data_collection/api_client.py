"""
api_client.py - Client for interacting with the Swiss Transport API

This module provides functions to fetch data from the Swiss Transport API,
including station information, departure/arrival boards, and connections.
"""

import requests
import time
import json
import os
import logging
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("api_client.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# API endpoints
BASE_URL = "http://transport.opendata.ch/v1"
STATIONBOARD_ENDPOINT = f"{BASE_URL}/stationboard"
CONNECTIONS_ENDPOINT = f"{BASE_URL}/connections"
LOCATIONS_ENDPOINT = f"{BASE_URL}/locations"

# Rate limiting parameters
MAX_REQUESTS_PER_DAY = {
    "connections": 1000,
    "stationboard": 10080
}
REQUEST_COUNTER = {
    "connections": 0,
    "stationboard": 0,
    "locations": 0,
    "last_reset": datetime.now().date()
}

# Rate limit tracking
RATE_LIMIT_EXCEEDED = {
    "connections": False,
    "stationboard": False,
    "locations": False,
    "reset_time": datetime.now()
}

# Cache to minimize redundant requests
API_CACHE = {}
CACHE_EXPIRY = 3600  # Cache expiry in seconds

# Backoff strategy parameters
MAX_RETRIES = 3
INITIAL_BACKOFF = 5  # seconds
MAX_BACKOFF = 60  # seconds


def _reset_counter_if_new_day():
    """Reset the request counters if it's a new day."""
    today = datetime.now().date()
    if REQUEST_COUNTER["last_reset"] < today:
        for key in REQUEST_COUNTER:
            if key != "last_reset":
                REQUEST_COUNTER[key] = 0
        REQUEST_COUNTER["last_reset"] = today
        logger.info("Request counters reset for the new day.")


def _check_rate_limit(endpoint_type: str) -> bool:
    """
    Check if we've exceeded the rate limit for the given endpoint type.
    
    Args:
        endpoint_type: Type of endpoint (connections, stationboard)
        
    Returns:
        bool: True if we can proceed, False if we've hit the rate limit
    """
    _reset_counter_if_new_day()
    
    # Check if we're in a rate-limited state
    if RATE_LIMIT_EXCEEDED[endpoint_type]:
        # Check if the rate limit reset time has passed
        if datetime.now() > RATE_LIMIT_EXCEEDED["reset_time"]:
            logger.info(f"Rate limit cool-down period ended for {endpoint_type}. Attempting requests again.")
            RATE_LIMIT_EXCEEDED[endpoint_type] = False
        else:
            logger.warning(f"Rate limit still in effect for {endpoint_type}. Waiting until {RATE_LIMIT_EXCEEDED['reset_time']}")
            return False

    # If we don't have a specific limit for this endpoint, allow the request
    if endpoint_type not in MAX_REQUESTS_PER_DAY:
        return True
        
    if REQUEST_COUNTER[endpoint_type] >= MAX_REQUESTS_PER_DAY[endpoint_type]:
        logger.warning(f"Rate limit exceeded for {endpoint_type}. Waiting until tomorrow.")
        return False
    
    return True


def _make_request(url: str, params: Dict, endpoint_type: str) -> Dict:
    """
    Make a request to the API with rate limiting, retry logic, and caching.
    
    Args:
        url: API endpoint URL
        params: Query parameters
        endpoint_type: Type of endpoint for rate limiting
        
    Returns:
        Dict: API response
    """
    # Check cache first
    cache_key = f"{url}_{json.dumps(params, sort_keys=True)}"
    cache_item = API_CACHE.get(cache_key)
    
    if cache_item and time.time() - cache_item['timestamp'] < CACHE_EXPIRY:
        logger.debug(f"Cache hit for {url}")
        return cache_item['data']
    
    # Check rate limit
    if not _check_rate_limit(endpoint_type):
        # If we hit the rate limit, return empty result
        return {"error": f"Rate limit exceeded for {endpoint_type}"}
    
    # Implement retry with exponential backoff
    retries = 0
    backoff_time = INITIAL_BACKOFF
    
    while retries <= MAX_RETRIES:
        try:
            # Add delay to prevent overwhelming the API
            delay = 0.5 + (random.random() * 0.5)  # 0.5-1s random delay
            time.sleep(delay)
            
            logger.info(f"Making request to {url} with params {params}")
            response = requests.get(url, params=params)
            
            # If successful, process as normal
            if response.status_code == 200:
                # Update request counter
                REQUEST_COUNTER[endpoint_type] += 1
                
                data = response.json()
                
                # Update cache
                API_CACHE[cache_key] = {
                    'data': data,
                    'timestamp': time.time()
                }
                
                return data
            
            # Handle rate limiting (429 Too Many Requests)
            elif response.status_code == 429:
                retries += 1
                
                # Get retry-after header or use exponential backoff
                retry_after = response.headers.get('Retry-After')
                if retry_after:
                    try:
                        wait_time = int(retry_after)
                    except (ValueError, TypeError):
                        wait_time = backoff_time
                else:
                    wait_time = backoff_time
                
                logger.warning(f"Rate limit hit (429) for {endpoint_type}. Retry {retries}/{MAX_RETRIES} after {wait_time} seconds")
                
                # Set the rate limit exceeded flag and reset time
                RATE_LIMIT_EXCEEDED[endpoint_type] = True
                RATE_LIMIT_EXCEEDED["reset_time"] = datetime.now() + timedelta(seconds=wait_time)
                
                if retries <= MAX_RETRIES:
                    time.sleep(wait_time)
                    # Increase backoff for next attempt
                    backoff_time = min(backoff_time * 2, MAX_BACKOFF)
                else:
                    logger.error(f"Max retries exceeded for {url}")
                    return {"error": f"Rate limit exceeded after {MAX_RETRIES} retries"}
            
            # Handle other errors
            else:
                response.raise_for_status()
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Error making request to {url}: {e}")
            
            retries += 1
            if retries <= MAX_RETRIES:
                logger.info(f"Retrying ({retries}/{MAX_RETRIES}) after {backoff_time} seconds")
                time.sleep(backoff_time)
                # Increase backoff for next attempt
                backoff_time = min(backoff_time * 2, MAX_BACKOFF)
            else:
                return {"error": str(e)}
    
    # If we've exhausted retries
    return {"error": f"Failed after {MAX_RETRIES} retries"}


def get_station_info(query: str) -> List[Dict]:
    """
    Get station information based on a search query.
    
    Args:
        query: Station name or part of it
        
    Returns:
        List[Dict]: List of matching stations with their details
    """
    params = {
        "query": query,
        "type": "station"
    }
    
    response = _make_request(LOCATIONS_ENDPOINT, params, "locations")
    if "error" in response:
        return []
    
    return response.get("stations", [])


def get_station_board(station: str, date: Optional[str] = None, 
                     time: Optional[str] = None, 
                     type_: str = "departure",
                     limit: int = 100) -> List[Dict]:
    """
    Get departure or arrival board for a station.
    
    Args:
        station: Station name or ID
        date: Date in format YYYY-MM-DD (optional)
        time: Time in format HH:MM (optional)
        type_: Either "departure" or "arrival"
        limit: Maximum number of results
        
    Returns:
        List[Dict]: List of departures/arrivals
    """
    params = {
        "station": station,
        "type": type_,
        "limit": limit
    }
    
    if date:
        params["date"] = date
    if time:
        params["time"] = time
    
    response = _make_request(STATIONBOARD_ENDPOINT, params, "stationboard")
    if "error" in response:
        return []
    
    return response.get("stationboard", [])


def get_connections(from_station: str, to_station: str, 
                   date: Optional[str] = None,
                   time: Optional[str] = None,
                   isArrivalTime: bool = False,
                   limit: int = 4) -> List[Dict]:
    """
    Get connections between two stations.
    
    Args:
        from_station: Origin station name or ID
        to_station: Destination station name or ID
        date: Date in format YYYY-MM-DD (optional)
        time: Time in format HH:MM (optional)
        isArrivalTime: If True, time is interpreted as arrival time
        limit: Maximum number of connections
        
    Returns:
        List[Dict]: List of connections
    """
    params = {
        "from": from_station,
        "to": to_station,
        "limit": limit
    }
    
    if date:
        params["date"] = date
    if time:
        params["time"] = time
    if isArrivalTime:
        params["isArrivalTime"] = "1"
    
    response = _make_request(CONNECTIONS_ENDPOINT, params, "connections")
    if "error" in response:
        return []
    
    return response.get("connections", [])


def clear_cache():
    """Clear the API response cache."""
    global API_CACHE
    API_CACHE = {}
    logger.info("API cache cleared.")


if __name__ == "__main__":
    # Simple test of the API client
    stations = get_station_info("Zürich")
    print(f"Found {len(stations)} stations matching 'Zürich'")
    
    if stations:
        station_id = stations[0]["id"]
        departures = get_station_board(station_id)
        print(f"Found {len(departures)} departures from {stations[0]['name']}")