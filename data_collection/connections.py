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

# Station pairs to focus on initially
CONNECTION_PAIRS = [
    ("Zürich HB", "Luzern"),
    ("Zürich HB", "Genève"),
    ("Luzern", "Genève")
]


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
    
    # Process departure and arrival times
    departure = safe_get(from_data, 'departure')
    departure_datetime = None
    if departure:
        try:
            departure_datetime = datetime.fromisoformat(departure.replace('Z', '+00:00'))
        except (ValueError, AttributeError) as e:
            logger.warning(f"Error parsing departure time {departure}: {e}")
    
    arrival = safe_get(to_data, 'arrival')
    arrival_datetime = None
    if arrival:
        try:
            arrival_datetime = datetime.fromisoformat(arrival.replace('Z', '+00:00'))
        except (ValueError, AttributeError) as e:
            logger.warning(f"Error parsing arrival time {arrival}: {e}")
    
    # Calculate duration
    duration_minutes = None
    if departure_datetime and arrival_datetime:
        duration = arrival_datetime - departure_datetime
        duration_minutes = duration.total_seconds() / 60
    
    # Extract sections (legs of the journey) with safe access
    sections = safe_get(connection, 'sections', [])
    if sections is None:
        sections = []
    
    transfers = len([s for s in sections if s is not None]) - 1 if sections else 0
    if transfers < 0:
        transfers = 0
    
    # Extract delay information
    departure_delay = safe_get(from_data, 'delay')
    arrival_delay = safe_get(to_data, 'delay')
    
    # Create processed entry
    processed = {
        'collection_date': datetime.now().strftime('%Y-%m-%d'),
        'collection_time': datetime.now().strftime('%H:%M:%S'),
        'from_station_id': safe_get(from_station, 'id', ''),
        'from_station_name': safe_get(from_station, 'name', ''),
        'to_station_id': safe_get(to_station, 'id', ''),
        'to_station_name': safe_get(to_station, 'name', ''),
        'departure_datetime': departure_datetime.strftime('%Y-%m-%d %H:%M:%S') if departure_datetime else None,
        'arrival_datetime': arrival_datetime.strftime('%Y-%m-%d %H:%M:%S') if arrival_datetime else None,
        'duration_minutes': duration_minutes,
        'transfers': transfers,
        'departure_delay': departure_delay,
        'arrival_delay': arrival_delay,
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
            processed[f'section_{i+1}_departure'] = safe_get(section_departure, 'departure')
            processed[f'section_{i+1}_departure_delay'] = safe_get(section_departure, 'delay')
            
            # Add section arrival info
            section_arrival = safe_get(section, 'arrival', {})
            section_to = safe_get(section_arrival, 'station', {})
            processed[f'section_{i+1}_to_id'] = safe_get(section_to, 'id', '')
            processed[f'section_{i+1}_to_name'] = safe_get(section_to, 'name', '')
            processed[f'section_{i+1}_arrival'] = safe_get(section_arrival, 'arrival')
            processed[f'section_{i+1}_arrival_delay'] = safe_get(section_arrival, 'delay')
    
    return processed


def collect_connection_data(from_station: str, to_station: str, date: str, 
                           time_slots: List[str], data_dir: str = "data/raw") -> str:
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
                             data_dir: str = "data/raw") -> List[str]:
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
        time_slots = ['06:00', '08:00', '10:00', '12:00', '14:00', '16:00', '18:00', '20:00']
    
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
                              data_dir: str = "data/raw") -> List[str]:
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