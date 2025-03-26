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
from typing import List, Dict, Optional, Tuple
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


def process_connection(connection: Dict) -> Dict:
    """
    Process a single connection to extract relevant information.
    
    Args:
        connection: Raw connection data from the API
        
    Returns:
        Dict: Processed connection with only the relevant fields
    """
    # Extract basic connection info
    from_station = connection.get('from', {}).get('station', {})
    to_station = connection.get('to', {}).get('station', {})
    
    # Process departure and arrival times
    departure = connection.get('from', {}).get('departure', None)
    departure_datetime = None
    if departure:
        departure_datetime = datetime.fromisoformat(departure.replace('Z', '+00:00'))
    
    arrival = connection.get('to', {}).get('arrival', None)
    arrival_datetime = None
    if arrival:
        arrival_datetime = datetime.fromisoformat(arrival.replace('Z', '+00:00'))
    
    # Calculate duration
    duration_minutes = None
    if departure_datetime and arrival_datetime:
        duration = arrival_datetime - departure_datetime
        duration_minutes = duration.total_seconds() / 60
    
    # Extract sections (legs of the journey)
    sections = connection.get('sections', [])
    transfers = len(sections) - 1 if sections else 0
    
    # Extract delay information
    departure_delay = connection.get('from', {}).get('delay', None)
    arrival_delay = connection.get('to', {}).get('delay', None)
    
    # Create processed entry
    processed = {
        'collection_date': datetime.now().strftime('%Y-%m-%d'),
        'collection_time': datetime.now().strftime('%H:%M:%S'),
        'from_station_id': from_station.get('id', ''),
        'from_station_name': from_station.get('name', ''),
        'to_station_id': to_station.get('id', ''),
        'to_station_name': to_station.get('name', ''),
        'departure_datetime': departure_datetime.strftime('%Y-%m-%d %H:%M:%S') if departure_datetime else None,
        'arrival_datetime': arrival_datetime.strftime('%Y-%m-%d %H:%M:%S') if arrival_datetime else None,
        'duration_minutes': duration_minutes,
        'transfers': transfers,
        'departure_delay': departure_delay,
        'arrival_delay': arrival_delay,
        'products': ', '.join([section.get('journey', {}).get('category', '') for section in sections if 'journey' in section]),
        'capacity1st': connection.get('capacity1st', None),
        'capacity2nd': connection.get('capacity2nd', None),
    }
    
    # Add information about all sections (legs)
    for i, section in enumerate(sections):
        if 'journey' in section:
            journey = section['journey']
            processed[f'section_{i+1}_category'] = journey.get('category', '')
            processed[f'section_{i+1}_number'] = journey.get('number', '')
            processed[f'section_{i+1}_operator'] = journey.get('operator', '')
            
            # Add section departure info
            section_from = section.get('departure', {}).get('station', {})
            processed[f'section_{i+1}_from_id'] = section_from.get('id', '')
            processed[f'section_{i+1}_from_name'] = section_from.get('name', '')
            processed[f'section_{i+1}_departure'] = section.get('departure', {}).get('departure', None)
            processed[f'section_{i+1}_departure_delay'] = section.get('departure', {}).get('delay', None)
            
            # Add section arrival info
            section_to = section.get('arrival', {}).get('station', {})
            processed[f'section_{i+1}_to_id'] = section_to.get('id', '')
            processed[f'section_{i+1}_to_name'] = section_to.get('name', '')
            processed[f'section_{i+1}_arrival'] = section.get('arrival', {}).get('arrival', None)
            processed[f'section_{i+1}_arrival_delay'] = section.get('arrival', {}).get('delay', None)
    
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
        
        connections = get_connections(from_station, to_station, date=date, time=time_slot)
        
        for connection in connections:
            processed = process_connection(connection)
            all_connections.append(processed)
    
    # Save data to CSV
    if all_connections:
        df = pd.DataFrame(all_connections)
        df.to_csv(output_path, index=False)
        logger.info(f"Saved {len(all_connections)} connections to {output_path}")
        return output_path
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
        file_path = collect_connection_data(from_station, to_station, date, time_slots, data_dir)
        if file_path:
            output_files.append(file_path)
    
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
        files = collect_daily_connections(date_str, time_slots, data_dir)
        output_files.extend(files)
        current_date += timedelta(days=1)
    
    return output_files


if __name__ == "__main__":
    # Example usage
    # Collect connection data for January 1, 2025
    files = collect_daily_connections('2025-01-01')
    print(f"Collected data saved to {len(files)} files.")