"""
Data Collection Package for Swiss Train Delays Analysis

This package provides modules for collecting and processing data about
train delays from various sources, including the Swiss Transport API.
"""

from data_collection.api_client import (
    get_station_info,
    get_station_board,
    get_connections,
    clear_cache
)

from data_collection.station_board import (
    collect_station_data,
    collect_data_for_period,
    collect_monthly_data
)

from data_collection.connections import (
    collect_connection_data,
    collect_daily_connections,
    collect_monthly_connections
)

# from data_collection.historical_data import (
#     download_archive_file,
#     extract_archive,
#     process_archived_stationboard,
#     fetch_delay_info,
#     collect_monthly_historical_data
# )

__all__ = [
    'get_station_info',
    'get_station_board',
    'get_connections',
    'clear_cache',
    'collect_station_data',
    'collect_data_for_period',
    'collect_monthly_data',
    'collect_connection_data',
    'collect_daily_connections',
    'collect_monthly_connections',
    'download_archive_file',
    'extract_archive',
    'process_archived_stationboard',
    'fetch_delay_info',
    'collect_monthly_historical_data'
]