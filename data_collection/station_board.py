"""
station_board.py - Module for collecting and processing station board data

This module handles retrieving, processing, and saving data about
arrivals at train stations.
"""

import os
import csv
import logging
from datetime import datetime, timedelta
import pandas as pd
import requests
from typing import List, Dict, Optional, Any, Tuple
from pathlib import Path

# Import configuration
from config import TARGET_STATIONS, DEFAULT_RAW_DIR, DEFAULT_STATION_BOARD_LIMIT
from config import STATIONBOARD_ENDPOINT

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("station_board.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def timestamp_to_time_format(timestamp: Optional[int], format_str: str = '%H:%M:%S') -> Optional[str]:
    """
    Convert a Unix timestamp to a formatted time string.
    
    Args:
        timestamp: Unix timestamp (seconds since epoch)
        format_str: Format string for the output (default: HH:MM:SS)
        
    Returns:
        str: Formatted time string or None if timestamp is invalid
    """
    if timestamp is None:
        return None
    
    try:
        time_obj = datetime.fromtimestamp(timestamp)
        return time_obj.strftime(format_str)
    except (ValueError, TypeError, OverflowError) as e:
        logger.warning(f"Error converting timestamp {timestamp} to time format: {e}")
        return None


def safe_get(data: Any, key: str, default: Any = None) -> Any:
    """
    Safely get a value from a dictionary, even if the object is None.
    
    Args:
        data: Dictionary-like object or None
        key: Key to retrieve
        default: Default value if key doesn't exist or data is None
        
    Returns:
        Value associated with key or default
    """
    if data is None:
        return default
    return data.get(key, default)


def get_station_board_direct(station: str, 
                           datetime_str: Optional[str] = None,
                           type_: str = "departure",
                           limit: int = DEFAULT_STATION_BOARD_LIMIT) -> Dict:
    """
    Make a direct call to the stationboard API endpoint.
    
    Args:
        station: Station name or ID
        datetime_str: Combined date and time in format YYYY-MM-DD hh:mm
        type_: Either "departure" or "arrival"
        limit: Maximum number of results
        
    Returns:
        Dict: Full API response including stationboard data
    """
    params = {
        "station": station,
        "type": type_,
        "limit": limit
    }
    
    if datetime_str:
        params["datetime"] = datetime_str
    
    try:
        response = requests.get(STATIONBOARD_ENDPOINT, params=params)
        response.raise_for_status()  # Raise exception for 4XX/5XX responses
        
        return response.json()
    except Exception as e:
        logger.error(f"Error making request to stationboard API: {e}")
        return {"stationboard": []}


def parse_iso_datetime(datetime_str: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Parse an ISO format datetime string into date and time components.
    
    Args:
        datetime_str: ISO datetime string like "2025-01-01T12:00:00+0100"
        
    Returns:
        Tuple[str, str]: (date, time) as strings or (None, None) if invalid
    """
    if not datetime_str:
        return None, None
        
    try:
        dt = datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
        return dt.strftime('%Y-%m-%d'), dt.strftime('%H:%M:%S')
    except (ValueError, AttributeError) as e:
        logger.warning(f"Error parsing datetime {datetime_str}: {e}")
        return None, None


def find_station_in_pass_list(pass_list: List[Dict], station_id: str) -> Optional[Dict]:
    """
    Find the specified station in the train's pass list.
    
    Args:
        pass_list: List of stops in the train's journey
        station_id: ID of the station to find
        
    Returns:
        Dict: Stop information for the specified station or None if not found
    """
    if not pass_list or not isinstance(pass_list, list):
        return None
    
    for stop in pass_list:
        if not isinstance(stop, dict):
            continue
            
        station = safe_get(stop, 'station', {})
        if safe_get(station, 'id') == station_id:
            return stop
    
    return None


def process_stationboard_entry(entry: Dict, station_id: str) -> Dict:
    """
    Process a single stationboard entry to extract relevant information.
    
    Args:
        entry: Raw stationboard entry from the API
        station_id: ID of the current station (to extract proper arrival time)
        
    Returns:
        Dict: Processed entry with only the relevant fields
    """
    # Extract basic entry data
    stop = safe_get(entry, 'stop', {})
    station = safe_get(stop, 'station', {})
    
    # Extract train information
    train_category = safe_get(entry, 'category', '')
    train_number = safe_get(entry, 'number', '')
    train_operator = safe_get(entry, 'operator', '')
    train_to = safe_get(entry, 'to', '')
    
    # Get standard departure/arrival info from the stop
    # Note: These might be null for arrival entries
    departure_str = safe_get(stop, 'departure')
    departure_timestamp = safe_get(stop, 'departureTimestamp')
    arrival_str = safe_get(stop, 'arrival')
    arrival_timestamp = safe_get(stop, 'arrivalTimestamp')
    
    # Extract delay (note: this is directly in the stop object)
    delay = safe_get(stop, 'delay')  # This should now correctly get the delay value
    
    # Get platform info
    platform = safe_get(stop, 'platform', '')
    
    # Process prognosis (actual times) information
    prognosis = safe_get(stop, 'prognosis', {})
    prognosis_departure = safe_get(prognosis, 'departure')
    prognosis_arrival = safe_get(prognosis, 'arrival')
    prognosis_platform = safe_get(prognosis, 'platform')
    
    # Check passList for actual arrival time at this station
    pass_list = safe_get(entry, 'passList', [])
    current_station_stop = find_station_in_pass_list(pass_list, station_id)
    
    # If we found the current station in the pass list, use its arrival information
    if current_station_stop:
        # Override arrival info with data from the pass list
        if not arrival_str:
            arrival_str = safe_get(current_station_stop, 'arrival')
        if not arrival_timestamp:
            arrival_timestamp = safe_get(current_station_stop, 'arrivalTimestamp')
        
        # If delay isn't set, try to get it from the station stop
        if delay is None:
            delay = safe_get(current_station_stop, 'delay')
        
        # Get platform from the current station stop if not already set
        if not platform:
            platform = safe_get(current_station_stop, 'platform', '')
        
        # Get prognosis information if not already set
        current_prognosis = safe_get(current_station_stop, 'prognosis', {})
        if not prognosis_arrival:
            prognosis_arrival = safe_get(current_prognosis, 'arrival')
        if not prognosis_platform:
            prognosis_platform = safe_get(current_prognosis, 'platform')
    
    # Parse and format dates/times
    departure_date, departure_time = parse_iso_datetime(departure_str)
    arrival_date, arrival_time = parse_iso_datetime(arrival_str)
    
    # Process prognosis timestamps
    _, prognosis_departure_time = parse_iso_datetime(prognosis_departure)
    _, prognosis_arrival_time = parse_iso_datetime(prognosis_arrival)
    
    # If delay is not available, try to calculate it from prognosis vs scheduled times
    if delay is None and arrival_str and prognosis_arrival:
        try:
            scheduled = datetime.fromisoformat(arrival_str.replace('Z', '+00:00'))
            actual = datetime.fromisoformat(prognosis_arrival.replace('Z', '+00:00'))
            delay = int((actual - scheduled).total_seconds() / 60)
        except (ValueError, AttributeError, TypeError) as e:
            logger.warning(f"Error calculating arrival delay: {e}")
    elif delay is None and departure_str and prognosis_departure:
        try:
            scheduled = datetime.fromisoformat(departure_str.replace('Z', '+00:00'))
            actual = datetime.fromisoformat(prognosis_departure.replace('Z', '+00:00'))
            delay = int((actual - scheduled).total_seconds() / 60)
        except (ValueError, AttributeError, TypeError) as e:
            logger.warning(f"Error calculating departure delay: {e}")
    
    # Create processed entry
    processed_entry = {
        'collection_date': datetime.now().strftime('%Y-%m-%d'),
        'collection_time': datetime.now().strftime('%H:%M:%S'),
        'station_id': safe_get(station, 'id', ''),
        'station_name': safe_get(station, 'name', ''),
        'train_category': train_category,
        'train_number': train_number,
        'train_operator': train_operator,
        'train_to': train_to,
        'departure_date': departure_date,
        'departure_time': departure_time,
        'arrival_date': arrival_date,
        'arrival_time': arrival_time,
        # Store timestamps as readable times
        'departure_timestamp': timestamp_to_time_format(departure_timestamp),
        'arrival_timestamp': timestamp_to_time_format(arrival_timestamp),
        'delay': delay,
        'platform': platform,
        'prognosis_departure': prognosis_departure_time,
        'prognosis_arrival': prognosis_arrival_time,
        'prognosis_platform': prognosis_platform,
        'capacity1st': safe_get(entry, 'capacity1st'),
        'capacity2nd': safe_get(entry, 'capacity2nd'),
    }
    
    # Add previous and next stop information (useful for arrival analysis)
    if pass_list and len(pass_list) > 1:
        # Try to find the index of the current station
        current_idx = -1
        for i, stop_info in enumerate(pass_list):
            station_info = safe_get(stop_info, 'station', {})
            if safe_get(station_info, 'id') == station_id:
                current_idx = i
                break
        
        # If found, get previous and next stops
        if current_idx >= 0:
            # Previous stop
            if current_idx > 0:
                prev_stop = pass_list[current_idx - 1]
                prev_station = safe_get(prev_stop, 'station', {})
                processed_entry['prev_station_id'] = safe_get(prev_station, 'id', '')
                processed_entry['prev_station_name'] = safe_get(prev_station, 'name', '')
                dep_date, dep_time = parse_iso_datetime(safe_get(prev_stop, 'departure'))
                processed_entry['prev_station_departure_time'] = dep_time
            
            # Next stop
            if current_idx < len(pass_list) - 1:
                next_stop = pass_list[current_idx + 1]
                next_station = safe_get(next_stop, 'station', {})
                processed_entry['next_station_id'] = safe_get(next_station, 'id', '')
                processed_entry['next_station_name'] = safe_get(next_station, 'name', '')
                arr_date, arr_time = parse_iso_datetime(safe_get(next_stop, 'arrival'))
                processed_entry['next_station_arrival_time'] = arr_time
    
    return processed_entry


def extract_data_from_stationboard(api_response: Dict, station_id: str) -> List[Dict]:
    """
    Extract processed entries from the stationboard API response.
    
    Args:
        api_response: Complete API response including stationboard data
        station_id: ID of the current station
        
    Returns:
        List[Dict]: List of processed entries
    """
    processed_entries = []
    stationboard_data = safe_get(api_response, 'stationboard', [])
    
    for entry in stationboard_data:
        try:
            processed = process_stationboard_entry(entry, station_id)
            processed_entries.append(processed)
        except Exception as e:
            logger.error(f"Error processing stationboard entry: {e}")
            # Continue with next entry
    
    return processed_entries


def generate_hourly_slots():
    """
    Generate hourly time slots from 05:00 to 23:59.
    
    Returns:
        List[str]: List of time slots in format HH:MM
    """
    slots = []
    for hour in range(5, 24):  # 5 AM to 11 PM
        slots.append(f"{hour:02d}:00")
    return slots


def collect_station_data(station_name: str, station_id: str, date: Optional[str] = None, 
                        time_window_days: int = 1, data_dir: str = DEFAULT_RAW_DIR) -> str:
    """
    Collect station board data for a given station over a time period.
    
    Args:
        station_name: Name of the station
        station_id: ID of the station
        date: Starting date in format YYYY-MM-DD
        time_window_days: Number of days to collect data for
        data_dir: Directory to save data
        
    Returns:
        str: Path to the saved CSV file
    """
    if date is None:
        # Default to today
        date = datetime.now().strftime('%Y-%m-%d')
    
    # Generate hourly time slots from 05:00 to 23:59
    time_slots = generate_hourly_slots()
    
    # Create date object and determine month
    start_date = datetime.strptime(date, '%Y-%m-%d')
    month_str = start_date.strftime('%Y-%m')
    
    # Create folder structure
    month_dir = os.path.join(data_dir, month_str)
    os.makedirs(month_dir, exist_ok=True)
    
    # Define output file path
    filename = f"{station_name.replace(' ', '_')}_{start_date.strftime('%Y-%m-%d')}.csv"
    output_path = os.path.join(month_dir, filename)
    
    all_entries = []
    
    # Collect data for the specified number of days
    current_date = start_date
    for _ in range(time_window_days):
        date_str = current_date.strftime('%Y-%m-%d')
        
        # Collect data for each hour from 05:00 to 23:59
        for time_slot in time_slots:
            # Format full datetime for API request
            datetime_param = f"{date_str} {time_slot}"
            logger.info(f"Collecting arrival data for {station_name} on {datetime_param}")
            
            try:
                # Make direct API call with datetime parameter
                api_response = get_station_board_direct(
                    station_id, 
                    datetime_str=datetime_param,
                    type_="arrival",
                    limit=DEFAULT_STATION_BOARD_LIMIT
                )
                
                arrival_entries = extract_data_from_stationboard(api_response, station_id)
                
                for entry in arrival_entries:
                    entry['board_type'] = 'arrival'
                    entry['request_time'] = time_slot  # Add time slot for reference
                    entry['request_date'] = date_str   # Add date for reference
                    all_entries.append(entry)
                    
                logger.info(f"Collected {len(arrival_entries)} arrival entries for {station_name} at {time_slot}")
            except Exception as e:
                logger.error(f"Error collecting arrival data for {station_name} on {datetime_param}: {e}")
        
        # Move to next day
        current_date += timedelta(days=1)
    
    # Save data to CSV
    if all_entries:
        try:
            df = pd.DataFrame(all_entries)
            df.to_csv(output_path, index=False)
            logger.info(f"Saved {len(all_entries)} entries to {output_path}")
            return output_path
        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")
            return ""
    else:
        logger.warning(f"No data collected for {station_name}")
        return ""


def collect_data_for_period(start_date: str, end_date: str, data_dir: str = DEFAULT_RAW_DIR) -> List[str]:
    """
    Collect data for all target stations for a specific period.
    
    Args:
        start_date: Start date in format YYYY-MM-DD
        end_date: End date in format YYYY-MM-DD
        data_dir: Directory to save data
        
    Returns:
        List[str]: Paths to saved CSV files
    """
    # Convert dates to datetime
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    # Calculate days between start and end
    delta = end - start
    days = delta.days + 1  # Include the end date
    
    output_files = []
    
    # Collect data for each station
    for station_name, station_id in TARGET_STATIONS.items():
        logger.info(f"Collecting data for {station_name} from {start_date} to {end_date}")
        
        # Process data month by month
        current_date = start
        while current_date <= end:
            # Determine the end of the current month or the end date, whichever comes first
            month_end = min(
                datetime(current_date.year, current_date.month, 1) + timedelta(days=32),
                end + timedelta(days=1)
            ).replace(day=1) - timedelta(days=1)
            
            # Calculate days in this segment
            segment_days = (month_end - current_date).days + 1
            
            # Collect data for this month
            file_path = collect_station_data(
                station_name, 
                station_id, 
                current_date.strftime('%Y-%m-%d'),
                segment_days, 
                data_dir
            )
            
            if file_path:
                output_files.append(file_path)
            
            # Move to the next month
            current_date = month_end + timedelta(days=1)
    
    return output_files


def collect_monthly_data(year: int, month: int, data_dir: str = DEFAULT_RAW_DIR) -> List[str]:
    """
    Collect data for all target stations for a specific month.
    
    Args:
        year: Year (e.g., 2025)
        month: Month (1-12)
        data_dir: Directory to save data
        
    Returns:
        List[str]: Paths to saved CSV files
    """
    # Determine start and end dates for the month
    start_date = datetime(year, month, 1)
    
    # Get the last day of the month
    if month == 12:
        end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = datetime(year, month + 1, 1) - timedelta(days=1)
    
    return collect_data_for_period(
        start_date.strftime('%Y-%m-%d'),
        end_date.strftime('%Y-%m-%d'),
        data_dir
    )


if __name__ == "__main__":
    # Example usage
    # Collect data for current date
    today = datetime.now().strftime('%Y-%m-%d')
    for station_name, station_id in TARGET_STATIONS.items():
        file_path = collect_station_data(station_name, station_id, today)
        if file_path:
            print(f"Data for {station_name} saved to {file_path}")