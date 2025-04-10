import pandas as pd
import numpy as np
from datetime import datetime

def load_historical_data(file_path='data/historical_transformed.csv'):
    """
    Load the historical train delay data from CSV
    """
    try:
        df = pd.read_csv(file_path)
        
        # Convert date strings to datetime objects if needed
        if 'arrival_planned' in df.columns and isinstance(df['arrival_planned'].iloc[0], str):
            df['arrival_planned'] = pd.to_datetime(df['arrival_planned'])
        
        # Extract additional time features if needed
        if 'arrival_planned' in df.columns:
            df['hour'] = df['arrival_planned'].dt.hour
            df['day_of_week'] = df['arrival_planned'].dt.dayofweek
            df['day_name'] = df['arrival_planned'].dt.day_name()
            df['month'] = df['arrival_planned'].dt.month
            df['year'] = df['arrival_planned'].dt.year
            
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        # Return empty DataFrame with expected columns to avoid app crashes
        return pd.DataFrame(columns=['station_name', 'train_category', 'delay_category', 
                                     'delay', 'arrival_planned', 'hour', 'day_of_week',
                                     'day_name', 'month', 'year'])

def filter_data(df, stations=None, categories=None, start_date=None, end_date=None):
    """
    Filter the dataframe based on selected criteria
    """
    filtered_df = df.copy()
    
    # Filter by station
    if stations and 'station_name' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['station_name'].isin(stations)]
    
    # Filter by train category
    if categories and 'train_category' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['train_category'].isin(categories)]
    
    # Filter by date range
    if start_date and end_date and 'arrival_planned' in filtered_df.columns:
        filtered_df = filtered_df[(filtered_df['arrival_planned'] >= start_date) & 
                                 (filtered_df['arrival_planned'] <= end_date)]
    
    return filtered_df

def calculate_delay_stats(df):
    """
    Calculate various delay statistics
    """
    if df.empty or 'delay' not in df.columns:
        return {
            'avg_delay': 0,
            'max_delay': 0,
            'min_delay': 0,
            'pct_delayed': 0,
            'pct_on_time': 0,
            'total_trains': 0
        }
    
    stats = {
        'avg_delay': df['delay'].mean(),
        'max_delay': df['delay'].max(),
        'min_delay': df['delay'].min(),
        'pct_delayed': (df['delay'] > 2).mean() * 100,  # % of trains delayed by more than 2 minutes
        'pct_on_time': (df['delay'] <= 2).mean() * 100,  # % of trains on time (≤ 2 minutes delay)
        'total_trains': len(df)
    }
    
    return stats

def get_delay_by_time(df, time_unit='hour'):
    """
    Group delays by hour, day of week, or month
    """
    if df.empty:
        return pd.DataFrame()
    
    if time_unit == 'hour' and 'hour' in df.columns:
        result = df.groupby('hour').agg(
            avg_delay=('delay', 'mean'),
            pct_delayed=('delay', lambda x: (x > 2).mean() * 100),
            count=('delay', 'count')
        ).reset_index()
        
    elif time_unit == 'day' and 'day_of_week' in df.columns:
        result = df.groupby(['day_of_week', 'day_name']).agg(
            avg_delay=('delay', 'mean'),
            pct_delayed=('delay', lambda x: (x > 2).mean() * 100),
            count=('delay', 'count')
        ).reset_index()
        result = result.sort_values('day_of_week')
        
    elif time_unit == 'month' and 'month' in df.columns:
        result = df.groupby('month').agg(
            avg_delay=('delay', 'mean'),
            pct_delayed=('delay', lambda x: (x > 2).mean() * 100),
            count=('delay', 'count')
        ).reset_index()
        
    else:
        result = pd.DataFrame()
        
    return result

def get_delay_by_station_and_category(df):
    """
    Group data by station and train category to analyze performance
    """
    if df.empty or 'station_name' not in df.columns or 'train_category' not in df.columns:
        return pd.DataFrame()
    
    result = df.groupby(['station_name', 'train_category']).agg(
        avg_delay=('delay', 'mean'),
        pct_delayed=('delay', lambda x: (x > 2).mean() * 100),
        count=('delay', 'count')
    ).reset_index()
    
    return result

def get_delay_categories_distribution(df):
    """
    Calculate the distribution of delay categories
    """
    if df.empty or 'delay_category' not in df.columns:
        return pd.DataFrame()
    
    # Count occurrences of each delay category
    result = df['delay_category'].value_counts().reset_index()
    result.columns = ['delay_category', 'count']
    
    # Calculate percentages
    result['percentage'] = result['count'] / result['count'].sum() * 100
    
    return result