import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

def generate_dummy_data(output_file='data/historical_transformed.csv', num_records=100000):
    """
    Generate synthetic train delay data that mimics the structure of historical SBB data
    
    Parameters:
    output_file (str): Path to save the generated CSV file
    num_records (int): Number of records to generate
    """
    print(f"Generating {num_records} synthetic train delay records...")
    
    # Define constants for data generation
    stations = ['Zürich HB', 'Luzern', 'Genève']
    
    train_categories = [
        'IC', 'IR', 'S', 'RE', 'EC', 'NJ', 'RJX', 'TGV', 'ICE', 'R',
        'EXT', 'EN', 'C', 'SN', 'KS', 'CE'
    ]
    
    # Assign different delay distributions to train categories
    category_delay_params = {
        'NJ': (20, 15),    # Night train (higher mean, higher std)
        'RJX': (15, 12),   # RailJet Express
        'IC': (10, 8),     # InterCity
        'IR': (8, 6),      # InterRegio
        'TGV': (12, 10),   # French high-speed train
        'EC': (10, 8),     # EuroCity
        'ICE': (9, 7),     # German high-speed train
        'EXT': (11, 9),    # Special train
        'RE': (6, 5),      # RegioExpress 
        'R': (3, 3),       # Regional train
        'EN': (18, 14),    # EuroNight
        'C': (2, 2),       # City train
        'S': (1, 1),       # S-Bahn (commuter train)
        'SN': (1, 1),      # S-Bahn night
        'KS': (4, 4),      # Short-distance
        'CE': (5, 5)       # Special event train
    }
    
    # Define delay categories
    def get_delay_category(delay):
        if delay <= 2:
            return 'On time'
        elif delay <= 5:
            return '2 to 5minutes'
        elif delay <= 15:
            return '5 to 15minutes'
        elif delay >= 100:  # Cancelled trains
            return 'Cancelled'
        else:
            return 'more than 15minutes'
    
    # Station weights (frequency of each station in the data)
    station_weights = {
        'Zürich HB': 0.5,  # 50% of data
        'Luzern': 0.3,     # 30% of data
        'Genève': 0.2      # 20% of data
    }
    
    # Setup the date range
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2024, 12, 31)
    date_range_days = (end_date - start_date).days
    
    # Generate the data
    data = []
    
    for _ in range(num_records):
        # Random station based on weights
        station = random.choices(list(station_weights.keys()), 
                                weights=list(station_weights.values()))[0]
        
        # Random train category
        train_category = random.choice(train_categories)
        
        # Random date within range
        random_days = random.randint(0, date_range_days)
        date = start_date + timedelta(days=random_days)
        
        # Random time that follows typical patterns
        # More trains during peak hours (7-9 AM, 4-7 PM)
        hour_weights = [
            0.01, 0.01, 0.01, 0.02, 0.03, 0.06,  # 0-5 AM
            0.08, 0.12, 0.10, 0.05, 0.04, 0.04,  # 6-11 AM
            0.04, 0.04, 0.04, 0.06, 0.08, 0.10,  # 12-5 PM
            0.08, 0.06, 0.04, 0.03, 0.02, 0.01   # 6-11 PM
        ]
        hour = random.choices(range(24), weights=hour_weights)[0]
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        
        # Create arrival_planned timestamp
        arrival_planned = date.replace(hour=hour, minute=minute, second=second)
        
        # Generate delay based on train category
        # Different train types have different delay distributions
        mean_delay, std_delay = category_delay_params.get(train_category, (5, 5))
        
        # Adjust for time of day (peak hours have more delays)
        if 7 <= hour <= 9 or 16 <= hour <= 19:
            mean_delay *= 1.3
            std_delay *= 1.2
        
        # Adjust for day of week (weekdays more delays than weekends)
        weekday = arrival_planned.weekday()
        if weekday >= 5:  # weekend
            mean_delay *= 0.7
            std_delay *= 0.7
        
        # Randomly decide if this is a cancelled train (1% probability)
        is_cancelled = random.random() < 0.01
        
        if is_cancelled:
            delay = 999  # Use a high value to represent cancellation
        else:
            # Generate delay value with some negative values (early arrivals)
            delay = np.random.normal(mean_delay, std_delay)
            # 10% of trains arrive early (negative delay)
            if random.random() < 0.1:
                delay = -abs(delay / 4)  # Early trains are not as early as late trains are late
        
        # Round delay to 1 decimal
        delay = round(delay, 1)
        
        # Assign delay category
        delay_category = get_delay_category(delay)
        
        # Calculate arrival_actual based on delay
        arrival_actual = arrival_planned + timedelta(minutes=delay) if not is_cancelled else None
        
        # Add record to dataset
        data.append({
            'station_name': station,
            'train_category': train_category,
            'arrival_planned': arrival_planned,
            'arrival_actual': arrival_actual,
            'delay': delay,
            'delay_category': delay_category,
            'is_cancelled': is_cancelled
        })
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Add derived columns for analysis
    df['hour'] = df['arrival_planned'].dt.hour
    df['day_of_week'] = df['arrival_planned'].dt.dayofweek
    df['day_name'] = df['arrival_planned'].dt.day_name()
    df['month'] = df['arrival_planned'].dt.month
    df['year'] = df['arrival_planned'].dt.year
    df['date'] = df['arrival_planned'].dt.date
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Save to CSV
    df.to_csv(output_file, index=False)
    print(f"Generated {len(df)} records and saved to {output_file}")
    
    # Display sample and summary
    print("\nSample data (first 5 rows):")
    print(df.head().to_string())
    
    print("\nData summary:")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    print(f"Stations: {df['station_name'].unique().tolist()}")
    print(f"Train categories: {df['train_category'].unique().tolist()}")
    print(f"Delay categories distribution:")
    print(df['delay_category'].value_counts(normalize=True).mul(100).round(1).astype(str) + '%')
    
    return df

if __name__ == "__main__":
    # Generate 100,000 records by default
    generate_dummy_data(num_records=100000)