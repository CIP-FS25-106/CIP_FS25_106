"""
connections.py - Module for collecting and processing connection data

This module handles retrieving, processing, and saving data about
connections between train stations.
"""

import os
import csv
import logging
from datetime import datetime, timedelta
import pandas as pd
from typing import List, Dict, Optional, Tuple, Any
from pathlib import Path

# Import from the same package
from data_collection.api_client import get_connections

# Import configuration
from config import CONNECTION_PAIRS, DEFAULT_TIME_SLOTS, DEFAULT_RAW_DIR

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("connections.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def safe_get(obj: Any, key: str, default: Any = None) -> Any:
    """
    Safely get a value from a dictionary, even if the object is None.
    
    Args:
        obj: Dictionary-like object or None
        key: Key to retrieve
        default: Default value if key doesn't exist or obj is None
        
    Returns:
        Value associated with key or default
    """
    if obj is None:
        return default
    return obj.get(key, default)


def parse_timestamp(timestamp: Optional[int]) -> Optional[datetime]:
    """
    Convert a Unix timestamp to a datetime object.
    
    Args:
        timestamp: Unix timestamp (seconds since epoch)
        
    Returns:
        datetime: Datetime object or None if timestamp is invalid
    """
    if timestamp is None:
        return None
    
    try:
        return datetime.fromtimestamp(timestamp)
    except (ValueError, TypeError, OverflowError) as e:
        logger.warning(f"Error parsing timestamp {timestamp}: {e}")
        return None


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


def parse_iso_datetime(datetime_str: Optional[str]) -> Optional[datetime]:
    """
    Parse an ISO format datetime string.
    
    Args:
        datetime_str: ISO datetime string like "2025-01-01T12:00:00+0100"
        
    Returns:
        datetime: Datetime object or None if invalid
    """
    if not datetime_str:
        return None
        
    try:
        return datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
    except (ValueError, AttributeError) as e:
        logger.warning(f"Error parsing datetime {datetime_str}: {e}")
        return None


def parse_duration(duration_str: Optional[str]) -> Optional[int]:
    """
    Parse a duration string in format "XXdHH:MM:SS" to minutes.
    
    Args:
        duration_str: Duration string
        
    Returns:
        int: Duration in minutes or None if invalid
    """
    if not duration_str:
        return None
        
    try:
        # Handle days if present (format: "00dHH:MM:SS")
        days = 0
        if 'd' in duration_str:
            days_part, time_part = duration_str.split('d')
            days = int(days_part)
        else:
            time_part = duration_str
            
        # Parse hours, minutes, seconds
        hours, minutes, seconds = map(int, time_part.split(':'))
        
        # Calculate total minutes
        total_minutes = (days * 24 * 60) + (hours * 60) + minutes + (seconds / 60)
        return round(total_minutes)
    except (ValueError, AttributeError) as e:
        logger.warning(f"Error parsing duration {duration_str}: {e}")
        return None


def calculate_delay_from_timestamp(scheduled_time_str: Optional[str], timestamp: Optional[int]) -> Optional[int]:
    """
    Calculate delay in minutes by comparing scheduled time with timestamp.
    
    Args:
        scheduled_time_str: Scheduled time in ISO format
        timestamp: Actual time as Unix timestamp
        
    Returns:
        int: Delay in minutes (positive = delay, negative = early) or None if can't calculate
    """
    if not scheduled_time_str or timestamp is None:
        return None
    
    try:
        # Parse scheduled time and ensure it's timezone-aware
        scheduled_time = datetime.fromisoformat(scheduled_time_str.replace('Z', '+00:00'))
        
        # Parse actual time from timestamp (this creates a timezone-naive datetime)
        actual_time = parse_timestamp(timestamp)
        
        if not actual_time:
            return None
        
        # Make actual_time timezone-aware with the same timezone as scheduled_time
        # If scheduled_time has timezone info, use it; otherwise assume UTC
        if scheduled_time.tzinfo:
            # Create a timezone-aware datetime with the same timezone
            if actual_time.tzinfo is None:
                actual_time = actual_time.replace(tzinfo=scheduled_time.tzinfo)
        else:
            # If scheduled_time has no timezone, make both timezone-naive for comparison
            scheduled_time = scheduled_time.replace(tzinfo=None)
            
        # Calculate difference in minutes
        delay_seconds = (actual_time - scheduled_time).total_seconds()
        delay_minutes = round(delay_seconds / 60)
        
        return delay_minutes
    except (ValueError, AttributeError, TypeError) as e:
        logger.warning(f"Error calculating delay: {e}")
        return None


def process_connection(connection: Dict) -> Dict:
    """
    Process a single connection to extract relevant information.
    
    Args:
        connection: Raw connection data from the API
        
    Returns:
        Dict: Processed connection with only the relevant fields
    """
    # Extract basic connection info with safe access
    from_data = safe_get(connection, 'from', {})
    to_data = safe_get(connection, 'to', {})
    
    from_station = safe_get(from_data, 'station', {})
    to_station = safe_get(to_data, 'station', {})
    
    # Get scheduled departure and arrival times from API
    departure_str = safe_get(from_data, 'departure')
    arrival_str = safe_get(to_data, 'arrival')
    
    # Get timestamps
    departure_timestamp = safe_get(from_data, 'departureTimestamp')
    arrival_timestamp = safe_get(to_data, 'arrivalTimestamp')
    
    # Parse scheduled times to datetime objects
    departure_dt = parse_iso_datetime(departure_str) 
    arrival_dt = parse_iso_datetime(arrival_str)
    
    # Format datetime objects to standard format for storage
    departure_date = departure_dt.strftime('%Y-%m-%d') if departure_dt else None
    arrival_date = arrival_dt.strftime('%Y-%m-%d') if arrival_dt else None
    
    # Convert timestamps to HH:MM:SS format for all time fields
    departure_time = timestamp_to_time_format(departure_timestamp, '%H:%M:%S')
    arrival_time = timestamp_to_time_format(arrival_timestamp, '%H:%M:%S')
    
    # If timestamps didn't provide times, fall back to ISO datetime strings
    if departure_time is None and departure_dt:
        departure_time = departure_dt.strftime('%H:%M:%S')
    
    if arrival_time is None and arrival_dt:
        arrival_time = arrival_dt.strftime('%H:%M:%S')
    
    # Calculate delays
    # First try to get delays directly from API
    departure_delay = safe_get(from_data, 'delay')
    arrival_delay = safe_get(to_data, 'delay')
    
    # If not provided by API, calculate from timestamps
    if departure_delay is None and departure_timestamp is not None and departure_str:
        departure_delay = calculate_delay_from_timestamp(departure_str, departure_timestamp)
    
    if arrival_delay is None and arrival_timestamp is not None and arrival_str:
        arrival_delay = calculate_delay_from_timestamp(arrival_str, arrival_timestamp)
    
    # Calculate connection delay (difference between departure and arrival delays)
    # This represents how much delay accumulated or was recovered during the journey
    travel_delay = None
    if departure_delay is not None and arrival_delay is not None:
        travel_delay = arrival_delay - departure_delay
    
    # Get duration information
    duration_str = safe_get(connection, 'duration')
    duration_minutes = parse_duration(duration_str)
    
    # Extract sections (legs of the journey) with safe access
    sections = safe_get(connection, 'sections', [])
    if sections is None:
        sections = []
    
    transfers = len([s for s in sections if s is not None]) - 1 if sections else 0
    if transfers < 0:
        transfers = 0
    
    # Create processed entry
    processed = {
        'collection_date': datetime.now().strftime('%Y-%m-%d'),
        'collection_time': datetime.now().strftime('%H:%M:%S'),
        'from_station_id': safe_get(from_station, 'id', ''),
        'from_station_name': safe_get(from_station, 'name', ''),
        'to_station_id': safe_get(to_station, 'id', ''),
        'to_station_name': safe_get(to_station, 'name', ''),
        'departure_date': departure_date,
        'arrival_date': arrival_date,
        'departure_time': departure_time,          # Now in HH:MM:SS format
        'arrival_time': arrival_time,              # Now in HH:MM:SS format
        'departure_timestamp': departure_time,     # Same as departure_time, in HH:MM:SS format
        'arrival_timestamp': arrival_time,         # Same as arrival_time, in HH:MM:SS format
        'duration_minutes': duration_minutes,
        'duration_str': duration_str,
        'transfers': transfers,
        'departure_delay': departure_delay,
        'arrival_delay': arrival_delay,
        'travel_delay': travel_delay,
        'departure_platform': safe_get(from_data, 'platform'),
        'arrival_platform': safe_get(to_data, 'platform'),
        'capacity1st': safe_get(connection, 'capacity1st'),
        'capacity2nd': safe_get(connection, 'capacity2nd'),
    }
    
    # Safely extract product categories
    product_categories = []
    for section in sections:
        if section is not None and 'journey' in section:
            category = safe_get(section['journey'], 'category', '')
            if category:
                product_categories.append(category)
    
    processed['products'] = ', '.join(product_categories)
    
    # Add information about all sections (legs)
    for i, section in enumerate(sections):
        if section is not None and 'journey' in section:
            journey = section['journey']
            processed[f'section_{i+1}_category'] = safe_get(journey, 'category', '')
            processed[f'section_{i+1}_number'] = safe_get(journey, 'number', '')
            processed[f'section_{i+1}_operator'] = safe_get(journey, 'operator', '')
            
            # Add section departure info
            section_departure = safe_get(section, 'departure', {})
            section_from = safe_get(section_departure, 'station', {})
            processed[f'section_{i+1}_from_id'] = safe_get(section_from, 'id', '')
            processed[f'section_{i+1}_from_name'] = safe_get(section_from, 'name', '')
            
            # Process departure details
            section_dept_str = safe_get(section_departure, 'departure')
            section_dept_ts = safe_get(section_departure, 'departureTimestamp')
            
            # Convert timestamp to HH:MM:SS format for all time fields
            section_dept_time = timestamp_to_time_format(section_dept_ts, '%H:%M:%S')
            
            # Fall back to ISO string if timestamp conversion fails
            if section_dept_time is None:
                section_dept_dt = parse_iso_datetime(section_dept_str)
                section_dept_time = section_dept_dt.strftime('%H:%M:%S') if section_dept_dt else None
            
            processed[f'section_{i+1}_departure_time'] = section_dept_time
            processed[f'section_{i+1}_departure_timestamp'] = section_dept_time  # Same as departure_time
            
            # Calculate section departure delay
            section_dept_delay = safe_get(section_departure, 'delay')
            if section_dept_delay is None and section_dept_ts and section_dept_str:
                section_dept_delay = calculate_delay_from_timestamp(section_dept_str, section_dept_ts)
            
            processed[f'section_{i+1}_departure_delay'] = section_dept_delay
            processed[f'section_{i+1}_departure_platform'] = safe_get(section_departure, 'platform')
            
            # Add section arrival info
            section_arrival = safe_get(section, 'arrival', {})
            section_to = safe_get(section_arrival, 'station', {})
            processed[f'section_{i+1}_to_id'] = safe_get(section_to, 'id', '')
            processed[f'section_{i+1}_to_name'] = safe_get(section_to, 'name', '')
            
            # Process arrival details
            section_arr_str = safe_get(section_arrival, 'arrival')
            section_arr_ts = safe_get(section_arrival, 'arrivalTimestamp')
            
            # Convert timestamp to HH:MM:SS format for all time fields
            section_arr_time = timestamp_to_time_format(section_arr_ts, '%H:%M:%S')
            
            # Fall back to ISO string if timestamp conversion fails
            if section_arr_time is None:
                section_arr_dt = parse_iso_datetime(section_arr_str)
                section_arr_time = section_arr_dt.strftime('%H:%M:%S') if section_arr_dt else None
            
            processed[f'section_{i+1}_arrival_time'] = section_arr_time
            processed[f'section_{i+1}_arrival_timestamp'] = section_arr_time  # Same as arrival_time
            
            # Calculate section arrival delay
            section_arr_delay = safe_get(section_arrival, 'delay')
            if section_arr_delay is None and section_arr_ts and section_arr_str:
                section_arr_delay = calculate_delay_from_timestamp(section_arr_str, section_arr_ts)
            
            processed[f'section_{i+1}_arrival_delay'] = section_arr_delay
            processed[f'section_{i+1}_arrival_platform'] = safe_get(section_arrival, 'platform')
            
            # Calculate section delay change
            if section_dept_delay is not None and section_arr_delay is not None:
                processed[f'section_{i+1}_delay_change'] = section_arr_delay - section_dept_delay
    
    return processed


def collect_connection_data(from_station: str, to_station: str, date: str, 
                           time_slots: List[str], data_dir: str = DEFAULT_RAW_DIR) -> str:
    """
    Collect connection data between two stations for a specific date and time slots.
    
    Args:
        from_station: Origin station name
        to_station: Destination station name
        date: Date in format YYYY-MM-DD
        time_slots: List of times in format HH:MM to check
        data_dir: Directory to save data
        
    Returns:
        str: Path to the saved CSV file
    """
    # Determine month directory
    date_obj = datetime.strptime(date, '%Y-%m-%d')
    month_str = date_obj.strftime('%Y-%m')
    
    # Create folder structure
    month_dir = os.path.join(data_dir, month_str)
    os.makedirs(month_dir, exist_ok=True)
    
    # Create sanitized names for the file
    from_name = from_station.replace(' ', '_')
    to_name = to_station.replace(' ', '_')
    
    # Define output file path
    filename = f"connection_{from_name}_to_{to_name}_{date}.csv"
    output_path = os.path.join(month_dir, filename)
    
    all_connections = []
    
    # Collect connections for each time slot
    for time_slot in time_slots:
        logger.info(f"Collecting connections from {from_station} to {to_station} at {date} {time_slot}")
        
        try:
            connections = get_connections(from_station, to_station, date=date, time=time_slot)
            
            for connection in connections:
                try:
                    processed = process_connection(connection)
                    all_connections.append(processed)
                except Exception as e:
                    logger.error(f"Error processing connection: {e}")
                    # Continue with next connection
        except Exception as e:
            logger.error(f"Error getting connections: {e}")
            # Continue with next time slot
    
    # Save data to CSV
    if all_connections:
        try:
            df = pd.DataFrame(all_connections)
            df.to_csv(output_path, index=False)
            logger.info(f"Saved {len(all_connections)} connections to {output_path}")
            return output_path
        except Exception as e:
            logger.error(f"Error saving connections to CSV: {e}")
            return ""
    else:
        logger.warning(f"No connections found from {from_station} to {to_station} on {date}")
        return ""


def collect_daily_connections(date: str, time_slots: Optional[List[str]] = None, 
                             data_dir: str = DEFAULT_RAW_DIR) -> List[str]:
    """
    Collect connection data for all station pairs for a specific date.
    
    Args:
        date: Date in format YYYY-MM-DD
        time_slots: List of times in format HH:MM to check
        data_dir: Directory to save data
        
    Returns:
        List[str]: Paths to saved CSV files
    """
    if time_slots is None:
        # Default to checking connections every 2 hours
        time_slots = DEFAULT_TIME_SLOTS
    
    output_files = []
    
    for from_station, to_station in CONNECTION_PAIRS:
        try:
            file_path = collect_connection_data(from_station, to_station, date, time_slots, data_dir)
            if file_path:
                output_files.append(file_path)
        except Exception as e:
            logger.error(f"Error collecting connection data for {from_station} to {to_station} on {date}: {e}")
            # Continue with next pair
    
    return output_files


def collect_monthly_connections(year: int, month: int, 
                              time_slots: Optional[List[str]] = None,
                              data_dir: str = DEFAULT_RAW_DIR) -> List[str]:
    """
    Collect connection data for all station pairs for a specific month.
    
    Args:
        year: Year (e.g., 2025)
        month: Month (1-12)
        time_slots: List of times in format HH:MM to check
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
    
    output_files = []
    current_date = start_date
    
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        try:
            files = collect_daily_connections(date_str, time_slots, data_dir)
            output_files.extend(files)
        except Exception as e:
            logger.error(f"Error collecting daily connections for {date_str}: {e}")
            # Continue with next day
        
        current_date += timedelta(days=1)
    
    return output_files


if __name__ == "__main__":
    # Example usage
    # Collect connection data for January 1, 2025
    files = collect_daily_connections('2025-01-01')
    print(f"Collected data saved to {len(files)} files.")