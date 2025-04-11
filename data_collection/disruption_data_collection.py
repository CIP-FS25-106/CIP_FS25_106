"""
data_collection_disruption_data.py - Module for collecting and processing disruption data

This module handles retrieving, processing, and saving data about the distruptions, their validity, cause, affected lines and stations.
"""


## Imports
import re
import pandas as pd
import requests
import os
import datetime


## Functions

def load_disruption_data(file, original_file):
    '''
    Loads train traffic data from a local file.

    - If 'delay_data_cleaned_wide.csv' exists, it loads that file.
    - Otherwise, it loads the original 'rail-traffic-information.parquet'.

    Args:
        file (str): File name of the processed and prepared file
        original_file (str): File name of the original file, still needs to be cleaned and processed

    Returns:
        pandas.DataFrame – The loaded train traffic data.
    '''

    if os.path.exists(file):
        print('Prepared file exists!')
        return pd.read_csv(file, sep=';', encoding="utf-8")
    else:
        print('Load data from original file!')
        historical_data = pd.read_parquet(original_file)
        return clean_delay_data(historical_data)

def getNewDataFromAPI(maxpublished, baseurl):
    """Fetches new data from the API.
    \n - Starts with offset 0, increases by 100 (max. 100 entries at a time)
    \n - Compares the 'published' timestamp of each entry to the provided date
    \n - Collects all entries with a more recent 'published' date, otherwise it stops

    Args:
        maxpublished (datetime): Latest Date published of the 'historical_data'.
        baseurl (str): Base URL of the API.

    Returns:
        pandas.DataFrame: All newly fetched data.
    """
    print('Getting new data...')
    offset = 0
    all_data = []

    # Convert the input 'published' date to a datetime object
    published_date = pd.to_datetime(maxpublished)

    cont = True

    while cont:
        try:
            print(f"Fetching data with offset {offset}...")

            # Get the data from the API with current offset
            response = requests.get(f"{baseurl}&offset={offset}")
            response.raise_for_status() # Check if fetching the new data was successful, otherwise raises an error
            response_data = response.json()

            newdata = pd.DataFrame(response_data['results'])

            # Check if the new data contains the columns 'published' and convert it to date format
            if 'published' not in newdata.columns:
                print(f"Error: The column published was not found! Please check: {baseurl}&offset={offset}"
                      f"\n If the offset is greather than 10000, you need to download the dataset from https://data.sbb.ch/explore/dataset/rail-traffic-information and start again")
                break
            else:
                newdata['published'] = pd.to_datetime(newdata['published'])

            # Iterate through the new entries as long the 'maxpublished' smaller is than the new entries 'published' date
            for _, entry in newdata.iterrows():
                entry_published = entry['published']
                if entry_published > published_date:
                    all_data.append(entry)

                else:
                    cont = False
                    break

            offset += 100

        except requests.exceptions.RequestException as e:
            print(f"Error with the API request: {e}")
            break

        except Exception as e:
            print(f"Unknown error: {e}")
            break

    return pd.DataFrame(all_data)

def clean_delay_data (data):
    """
    Cleans and preprocesses train disruption data:
    - Set data types
    - Handling missing values
    - Removes duplicate entries
    - Filtering out irrelevant rows
    - Feature engineering
    - Merge train delay data with station name and station id

    Args:
        data (pandas.DataFrame): Raw Train disruption data.

    Returns:
        pandas.DataFrame: Cleaned train disruption data, with new columns: reason, reason_group, station_name, line_info, etc.

    """

    # Drop not used columns
    data = data.drop(columns=['link', 'author', 'description_html'])

    # Set datatypes
    for col in ["published", "validitybegin", "validityend"]:
        data[col] = pd.to_datetime(data[col], errors="coerce", utc=True)

    ## Remove NA
    data = data.dropna(subset=["published", "validitybegin", "validityend", "title", "description"])

    ## Check duplicates
    ## Sort the data in ascending order by 'published' to ensure the earliest (oldest) entry comes first.
    data = data.sort_values(by='published', ascending=True)

    ## Drop duplicates by keeping the first occurrence for each unique entry (ignoring the 'published' column, as interruptions are reposted every night if planned).
    data = data.drop_duplicates(subset=data.columns.difference(['published']), keep='first')

    ## Sort the data in descending order by 'published' so that the most recent entries appear first.
    data = data.sort_values(by='published', ascending=False)

    ## Feature Engineering
    data = feature_engineering(data)

    ## Matching the Title with the Train Station Data
    data = get_stations(stations_df=station_data, delay_df=data)

    return data




def feature_engineering(delay_info_list):
    """
    This function adds additional features by calculating or processing the titel or description It add the following colums:
    - duration of the interruption
    - time from the publish to the start
    - time from the publish to the start in days
    - if the delay was planned or unplanned
    - reason for the delay
    - affected lines
    - removes irrelevant rows

    Args:
        delay_info_list (pandas.DataFrame): Cleaned Train Interruption Data

    Returns:
        pandas.DataFrame: Train Interruption Data
    """
    # Calculate the duration of the interruption
    delay_info_list['duration'] = delay_info_list['validityend'] - delay_info_list['validitybegin']

    # Calculate the between publishing the info and start of the interruption
    delay_info_list['timetostart'] = delay_info_list['validitybegin'] - delay_info_list['published']

    # Calculate the number of days to the start of the interruption
    delay_info_list['timetostart_days'] = delay_info_list['timetostart'].dt.days

    # Planned or unplanned
    # Published on the same das as the validity begins -> unplanned
    delay_info_list['planned'] = delay_info_list['timetostart_days'] > 0

    # Add the reason for the interruption with regex

    # REGEX: Grund dafür
    pattern = r"Der Grund dafür (?:ist ein|ist eine|ist|sind) (.*?)(?:\.|$)"
    delay_info_list['reason'] = delay_info_list['description'].apply(
        lambda x: re.search(pattern, x).group(1) if re.search(pattern, x) else None
    )

    # If no reason was able to be found, it will look for some words in the title or description
    # Define a dictionary with keywords and their corresponding reason labels
    reason_keywords = {
        "Bauarbeit": "Bauarbeiten",
        "Streik": "Streik",
        "Veranstaltung": "Veranstaltung",
        "Busersatz": "Busersatz"
    }
    # Loop through each keyword in the dictionary and update the 'reason' column
    for keyword, reason in reason_keywords.items():
        mask = delay_info_list['reason'].isna() & delay_info_list['description'].str.contains(keyword, case=False, na=False)
        delay_info_list.loc[mask, 'reason'] = reason

    ## Drop rows where 'title' contains "Aufgehoben" (Aufgehoben = Removed)
    delay_info_list = delay_info_list[~delay_info_list['title'].str.contains('Aufgehoben', case=False, na=False)]

    ## Translate and Group the Reasons for delay from German to English
    # Mapping of reasons to groups for REGEX
    pattern_map = {
        'Construction': r'(Bauarbeit|Wartungsarbeiten)',

        'External event': r'(Fremdereignis|Strassenfahrzeug|Kollision mit einem Fahrzeug|Unfall|Brand|Stromausfall|Entgleisung)',

        'Event': r'Veranstaltung',

        'Staff Shortage': r'Personalmangel',

        'Weather / Nature': r'(umgestürzter Baum|Unwetterschäden|Erdrutsch|vereiste Bahnanlagen|Unwetter|Erdrutschgefahr|Naturereignis|Steinschlag|witterung)',

        'Abroad': r'Ereignis in',

        'Protest': r'Streik',

        'Technical issue': r'(technische Störung|Fahrzeugstörung)',

        'Operational disruption': r'(Strecke blockiert|Fahrleitungsstörung|Gleisschaden|Barrierenstörung|Betriebsstörung)',

        'Replacement Bus:' : r'(Busersatz)',

        'Unknown': r'unbekannt'
    }

    # Function to categorize the reason based on regex patterns
    # Categorize reason by pattern
    def categorize_reason(reason):
        if not isinstance(reason, str):
            return 'Unknown'
        for group, pattern in pattern_map.items():
            if re.search(pattern, reason, re.IGNORECASE):
                return group
        return 'Other'

    # Apply the categorization function
    delay_info_list = delay_info_list.copy()
    delay_info_list['reason_group'] = delay_info_list['reason'].apply(categorize_reason)

    # Drop rows if reason is NA and drop_keywords are found
    drop_keywords = [
        "Fahrplanwechsel",
        "Reisehinweis",
        "Sitzplatzverfügbarkeit",
        "geänderten Fahrzeugeinsatz"
    ]
    pattern = '|'.join(drop_keywords)

    # Add the affected lines
    pattern = r"Betroffen (?:sind die Linien|ist die Linie) (.*?)(?:\.|$)"
    delay_info_list['affected_lines'] = delay_info_list['description'].apply(
        lambda x: re.search(pattern, x).group(1).split(', ') if re.search(pattern, x) else None
    )
    # Further split by " und " if it exists in any of the lines
    delay_info_list['affected_lines'] = delay_info_list['affected_lines'].apply(
        lambda lines: [line.strip() for affected_line in lines for line in
                       affected_line.split(' und ')] if lines else []
    )

    return delay_info_list


def get_stations(stations_df, delay_df):
    """
    This function searches through the titles in the dataframe `delay_df` and identifies
    which stations from the `stations_df` list are mentioned in each title. It returns
    the matching stations along with their IDs in a new column 'stations' in the dataframe.

    Parameters:
    - stations_df (pandas.DataFrame): A dataframe containing station details like 'designationofficial' and 'number'.
    - delay_df (pandas.DataFrame): A dataframe containing a column 'title' where the station names are to be searched for.

    Returns:
    - pandas.DataFrame: The original dataframe with an additional 'stations' column, which contains a list of station names and IDs found in the titles.
    """
    # Prepare a set of station names for faster lookup (for exact matching)
    stations_set = set(stations_df['designationofficial'].str.lower()) 
    station_map = {station['designationofficial'].lower(): station for _, station in stations_df.iterrows()}

    found_stations = []

    # Define regex pattern to match 'Einschränkung ' or 'Unterbruch ' in title
    pattern = re.compile(r"(Einschränkung|Unterbruch)\s+([A-Za-z0-9äöüÄÖÜß\s\-]+)")

    # Iterate over all titles in the Delay-DataFrame
    for title in delay_df['title']:
        stations_in_title = []  
        title_lower = title.lower()

        # Search for the pattern in the title (if the title contains 'Einschränkung' or 'Unterbruch')
        match = re.search(pattern, title)
        if match:
            # Extract the part after 'Einschränkung' or 'Unterbruch'
            rest_of_title = match.group(2)

            # Split by ' - ' and check if the parts exist in 'stations_df'
            stations_parts = [part.strip().lower() for part in rest_of_title.split(' - ')]

            # Check if each part exactly matches the station names from stations_set
            for part in stations_parts:
                if part in stations_set:
                    station_info = station_map[part]
                    stations_in_title.append({'id': station_info['number'], 'name': station_info['designationofficial']})

        # If no stations found, check if for station is directly in the title
        if not stations_in_title:
            for station_name, station_info in station_map.items():
                if station_name in title_lower:
                    stations_in_title.append({'id': station_info['number'], 'name': station_info['designationofficial']})

        # Format the found stations as a dict
        station_info_dict = {f"{s['id']}": s['name'] for s in stations_in_title}
        found_stations.append(station_info_dict)

    # Add the results to a new 'stations' column in the delay_df
    delay_df['stations'] = found_stations
    return delay_df



## Load Station Data
station_data = pd.read_parquet("haltestelle-haltekante.parquet")

## Load Historical Interruption Data
historical_data = load_disruption_data(file = "delay_data_cleaned_wide.csv", original_file = "rail-traffic-information.parquet")

## Load New Data
baseurl = "https://data.sbb.ch/api/explore/v2.1/catalog/datasets/rail-traffic-information/records?order_by=published%20desc&limit=100"
datafromAPI = getNewDataFromAPI(max(historical_data['published']), str(baseurl))

## Check if there is new data to be added to the historical data
if not datafromAPI.empty:
    # Clean and prepare the data
    datafromAPI_clean = clean_delay_data(datafromAPI)
    
    # Combine the Data from the API to the History Data
    combineddata = pd.concat([datafromAPI_clean, historical_data], ignore_index=True)

else:
    ## Copy the current historical data
    combineddata = historical_data.copy()

# Check Duplicates Again
combineddata['published'] = pd.to_datetime(combineddata['published'], errors='coerce')
combineddata['validitybegin'] = pd.to_datetime(combineddata['validitybegin'], errors='coerce')
combineddata['validityend'] = pd.to_datetime(combineddata['validityend'], errors='coerce')
combineddata['affected_lines'] =combineddata['affected_lines'].astype(str)
combineddata['stations'] =combineddata['stations'].astype(str)

combineddata = combineddata.sort_values(by='published', ascending=True)
combineddata = combineddata.drop_duplicates(
    subset=["title", 'validitybegin', 'validityend', "stations", "affected_lines", "reason"], keep='first'
)
combineddata = combineddata.sort_values(by='published', ascending=False)


# Save as CSV
combineddata.to_csv("delay_data_cleaned_wide.csv", index=False, sep=";", encoding="utf-8")






## Fucntions


def full_split(df):
    """
    This function transforms a DataFrame into a long format by:
    1. Creating separate rows for each day and hour within the time range defined by 'validitybegin' and 'validityend'.
    2. Splitting the 'stations' column, which contains station numbers and names, into separate columns for station number and station city.
    3. Splitting the 'affected_lines' column into individual lines.

    Args:
    df (pd.DataFrame): A pandas DataFrame containing disruption data with columns:
        - 'validitybegin': Timestamp of the beginning of the disruption period.
        - 'validityend': Timestamp of the end of the disruption period.
        - 'stations': A dictionary containing station numbers as keys and station cities as values.
        - 'affected_lines': A string with affected train lines, separated by commas.

    Returns:
    pd.DataFrame: A transformed DataFrame in long format with additional columns:
        - 'delay_day': Date for each day of the disruption.
        - 'hour': Hour of the disruption (in hour:minute format).
        - 'station_number': The station number.
        - 'station_city': The city of the station.
        - 'affected_lines': Each affected line as a separate row.

    """

    print("Starting preparing the long table....")

    ## Create for every hour a row
    all_rows = []

    for _, row in df.iterrows():
        start = pd.to_datetime(row["validitybegin"]).replace(minute=0, second=0)
        end = pd.to_datetime(row["validityend"])
        hours = pd.date_range(start, end, freq='h')

        for hour in hours:
            new_row = row.copy()
            new_row["delay_day"] = hour.date()
            new_row["hour"] = hour.strftime("%H:%M")
            all_rows.append(new_row)


    df = pd.DataFrame(all_rows)

    ## Split the Stations
    df["stations"] = df["stations"].astype(str).apply(eval)
    # Extract the number and station name into separate columns
    df['station_number'] = df['stations'].apply(lambda x: list(x.keys()) if isinstance(x, dict) else [])
    df['station_city'] = df['stations'].apply(lambda x: list(x.values()) if isinstance(x, dict) else [])

    # If you want to explode the rows based on the 'affected_lines', we can proceed with the explode operation
    df = df.explode('station_number').explode('station_city')

    ## Splitt the Lines
    df["affected_lines"] = df["affected_lines"].astype(str).apply(
        lambda x: [item.strip() for item in x.split(',') if item.strip()])
    df = df.explode("affected_lines")

    return df


## Make the data to long format
filepath = "delay_data_cleaned_long.csv"
if os.path.exists(filepath):
    print('File exists!')
    disruption_long = pd.read_csv(filepath, sep=';', encoding="utf-8")
    if not datafromAPI.empty:
        ## From here one we only use validity begin date > 1.1.2024, due to performance and also the data quality issue (see delay_reason_analysis.ipynb)
        disruption_long = disruption_long[disruption_long['validitybegin'] >= '2024-01-01']
        datafromAPI_clean_long = full_split(disruption_long[disruption_long['validitybegin'] >= '2024-01-01'])
        disruption_long = pd.concat([datafromAPI_clean_long, disruption_long], ignore_index=True)

else:
    print('File does not exist!')
    combineddata['validitybegin'] = pd.to_datetime(combineddata['validitybegin'], errors='coerce')
    disruption_long = combineddata[combineddata['validitybegin'] >= '2024-01-01']
    disruption_long = full_split(combineddata[combineddata['validitybegin'] >= '2024-01-01'])



## Drop Duplicates again
disruption_long = disruption_long.sort_values(by='published', ascending=True)
disruption_long = disruption_long.drop_duplicates(
    subset=["title", "delay_day", "hour", "station_number", "affected_lines", "reason"], keep='first'
)
disruption_long = disruption_long.sort_values(by='published', ascending=False)

## Write the CSV
disruption_long.to_csv("delay_data_cleaned_long.csv", index=False, sep=";", encoding="utf-8")
print('end')
