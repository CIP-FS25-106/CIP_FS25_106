"""
combining_disruption_trandata.py - Module for combining disruption data with the train connection data

"""

## Imports
import pandas as pd
from pathlib import Path
import numpy as np
import re


# Load CSV files
path = Path(__file__).parent
traindata = pd.read_csv('historical_transformed.csv')
delayinfo = pd.read_csv('delay_data_cleaned_long.csv', sep=';', encoding='utf-8')


# Extract selected stations from traindata
selected_stations = traindata['station_name'].unique()

# Preprocess train connection data
traindata['scheduled_arrival'] = pd.to_datetime(traindata['scheduled_arrival'])
traindata['day'] = pd.to_datetime(traindata['scheduled_arrival'])
traindata['delay_day'] = pd.to_datetime(traindata['scheduled_arrival']).dt.date
traindata['delay_hour'] = traindata['scheduled_arrival'].dt.floor("h").dt.time
traindata['scheduled_arrival_time'] = traindata['scheduled_arrival'].dt.time

# Filter traindata for the relevant date range
traindata = traindata[traindata['day'] >= pd.to_datetime('2024-01-01')]

# Preprocess delayinfo data
delayinfo = delayinfo[delayinfo['station_city'].isin(selected_stations)]
delayinfo['line_text'] = delayinfo['affected_lines']
delayinfo['line_text'] = delayinfo['line_text'].apply(lambda x: re.sub(r'[^a-zA-Z0-9]', '', str(x)))
delayinfo['station_name'] = delayinfo['station_city']
delayinfo['delay_day'] = pd.to_datetime(delayinfo['delay_day']).dt.date
delayinfo['delay_hour'] = pd.to_datetime(delayinfo['hour'], format="%H:%M").dt.floor("h").dt.time
delayinfo['validitybegin'] = pd.to_datetime(delayinfo['validitybegin'])
delayinfo['validityend'] = pd.to_datetime(delayinfo['validityend'])


# Merge datasets on station name, line text, delay day, and delay hour

combineddf = traindata.merge(
    delayinfo,
    how='left',
    on=['station_name', 'line_text', 'delay_day', 'delay_hour']
)


## Write CSV
combineddf.to_csv("train_disruption_data_combined_xxx.csv", index=False, sep=";", encoding='utf-8')


# List of columns to be cleared if disruption is not active in this hour
columns_to_clear = [
    "title", "description", "published", "validitybegin", "validityend", "duration",
    "timetostart", "timetostart_days", 'planned', "reason",
    "stations", "reason_group", "hour", "station_number", "station_city",  "affected_lines"
]
print(combineddf.info())
print(combineddf[['validitybegin', 'scheduled_arrival_time', 'validityend']])

combineddf['validitybegin'] = pd.to_datetime(combineddf['validitybegin'], errors='coerce')
combineddf['validityend'] = pd.to_datetime(combineddf['validityend'], errors='coerce')
combineddf['scheduled_arrival'] = pd.to_datetime(combineddf['scheduled_arrival'], errors='coerce')

combineddf['validitybegin'] = combineddf['validitybegin'].dt.tz_localize(None)
combineddf['validityend'] = combineddf['validityend'].dt.tz_localize(None)
combineddf['scheduled_arrival'] = combineddf['scheduled_arrival'].dt.tz_localize(None)

for idx, row in combineddf.iterrows():
    if pd.notna(row['validitybegin']):
        print(idx)
        print(row['validitybegin'] <= row['scheduled_arrival'] <= row['validityend'])
        print(row['validitybegin'] , row['scheduled_arrival'] , row['validityend'])
        if not (row['validitybegin'] <= row['scheduled_arrival'] <= row['validityend']):
            print(row)
            combineddf.loc[idx, columns_to_clear] = np.nan

print(combineddf.info())

# Merge only the 'planned_disruptions_at_station' column from 'delayinfo'
delayinfo['planned_disruptions_at_station'] = delayinfo['planned']
combineddf = combineddf.merge(
    delayinfo[['station_name', 'delay_day', 'delay_hour', 'planned_disruptions_at_station']],  # Select specific columns
    how='left',
    on=['station_name', 'delay_day', 'delay_hour']
)
print(combineddf.info())

## Through the merges above some new rows were created, these will now be dropped again
columns = ['ride_description', 'station_name', 'line_text', 'scheduled_arrival']

# Drop duplicates based on these columns
combineddf = combineddf.drop_duplicates(subset=columns)

combineddf.drop(columns=['title', 'description', 'timetostart', 'timetostart_days', 'affected_lines', 'stations'], inplace=True)

## Write CSV
combineddf.to_csv("train_disruption_data_combined.csv", index=False, sep=";", encoding='utf-8')
