import pandas as pd
import os
import glob
import numpy as np
from datetime import datetime
from pathlib import Path

# Get the path to the project root directory
project_root = Path(__file__).parent.parent.parent

# Define the stations to filter for
stations = ["Zürich HB", "Luzern", "Genève"]

# Columns to keep with their original names
columns_to_keep = [
    "BETRIEBSTAG", 
    "FAHRT_BEZEICHNER", 
    "BETREIBER_ABK",
    "PRODUKT_ID", 
    "LINIEN_TEXT",
    "VERKEHRSMITTEL_TEXT",
    "ZUSATZFAHRT_TF",
    "FAELLT_AUS_TF",
    "HALTESTELLEN_NAME",
    "ANKUNFTSZEIT",
    "AN_PROGNOSE",
    "AN_PROGNOSE_STATUS",
    "DURCHFAHRT_TF"
]

# Create output directory
output_dir = project_root / "data" / "historical" / "processed"
os.makedirs(output_dir, exist_ok=True)

# List of years to process
years = ["2022", "2023", "2024"]

# Initialize an empty list to store filtered DataFrames
all_filtered_chunks = []

# Process each year's data
for year in years:
    # Path to data folder for this year
    folder_path = project_root / "data" / "historical" / f"downloads_{year}"
    
    # Skip if folder doesn't exist
    if not folder_path.exists():
        print(f"Folder not found: {folder_path}")
        continue
        
    print(f"Processing data for year {year}...")
    
    # Get all CSV files in the folder
    csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
    
    # Process each CSV file
    for file_path in csv_files:
        print(f"Processing file: {os.path.basename(file_path)}")
        
        # Use chunking to process large files
        chunk_size = 100000
        
        # Create a CSV reader that processes chunks
        csv_reader = pd.read_csv(
            file_path, 
            sep=";",
            chunksize=chunk_size,
            usecols=columns_to_keep,
            low_memory=False
        )
        
        # Process each chunk
        for i, chunk in enumerate(csv_reader):
            # Apply all filters
            filtered_chunk = chunk[
                (chunk["BETREIBER_ABK"] == "SBB") &
                (chunk["PRODUKT_ID"] == "Zug") &
                (chunk["ZUSATZFAHRT_TF"] == False) &
                (chunk["DURCHFAHRT_TF"] == False) &
                (chunk["HALTESTELLEN_NAME"].isin(stations)) &
                (chunk["AN_PROGNOSE_STATUS"].notna())
            ]
            
            # Append the filtered chunk to our list
            if not filtered_chunk.empty:
                all_filtered_chunks.append(filtered_chunk)
        
        print(f"Finished processing {os.path.basename(file_path)}")

# Concatenate all filtered chunks into a single DataFrame
if all_filtered_chunks:
    final_df = pd.concat(all_filtered_chunks, ignore_index=True)
    
    # Convert BETRIEBSTAG to datetime
    final_df["BETRIEBSTAG"] = pd.to_datetime(final_df["BETRIEBSTAG"], format="%d.%m.%Y", dayfirst=True)
    
    # Extract just the time part from ANKUNFTSZEIT and AN_PROGNOSE
    final_df["ANKUNFTSZEIT_CLEAN"] = final_df["ANKUNFTSZEIT"].str.extract(r'(\d{2}:\d{2}(?::\d{2})?)')
    final_df["AN_PROGNOSE_CLEAN"] = final_df["AN_PROGNOSE"].str.extract(r'(\d{2}:\d{2}(?::\d{2})?)')
    
    # Combine date and clean time
    final_df["ANKUNFTSZEIT_DT"] = pd.to_datetime(
        final_df["BETRIEBSTAG"].dt.strftime('%Y-%m-%d') + ' ' + final_df["ANKUNFTSZEIT_CLEAN"]
    )
    
    final_df["AN_PROGNOSE_DT"] = pd.to_datetime(
        final_df["BETRIEBSTAG"].dt.strftime('%Y-%m-%d') + ' ' + final_df["AN_PROGNOSE_CLEAN"]
    )
    
    # Calculate delay in minutes
    final_df["DELAY"] = (final_df["AN_PROGNOSE_DT"] - final_df["ANKUNFTSZEIT_DT"]).dt.total_seconds() / 60
    
    # Create DELAY_CAT
    conditions = [
        final_df["FAELLT_AUS_TF"] == True,
        (final_df["DELAY"] > 2) & (final_df["DELAY"] <= 5),
        (final_df["DELAY"] > 5) & (final_df["DELAY"] <= 15),
        final_df["DELAY"] > 15
    ]
    
    choices = ["Cancelled", "2 to 5minutes", "5 to 15minutes", "more than 15minutes"]
    
    final_df["DELAY_CAT"] = np.select(conditions, choices, default="On time")
    
    # Drop temporary columns used for date/time processing
    final_df = final_df.drop(columns=["ANKUNFTSZEIT_CLEAN", "AN_PROGNOSE_CLEAN"])
    
    # Rename the datetime columns
    final_df = final_df.rename(columns={
        "ANKUNFTSZEIT_DT": "ANKUNFTSZEIT", 
        "AN_PROGNOSE_DT": "AN_PROGNOSE"
    })
    
    # Rename all columns to English
    column_mapping = {
        "BETRIEBSTAG": "ride_day",
        "FAHRT_BEZEICHNER": "ride_description",
        "BETREIBER_ABK": "train_operator",
        "PRODUKT_ID": "product_id",
        "LINIEN_TEXT": "line_text",
        "VERKEHRSMITTEL_TEXT": "train_category",
        "ZUSATZFAHRT_TF": "extra_train",
        "FAELLT_AUS_TF": "cancelled",
        "HALTESTELLEN_NAME": "station_name",
        "ANKUNFTSZEIT": "scheduled_arrival",
        "AN_PROGNOSE": "arrival_prognosis",
        "AN_PROGNOSE_STATUS": "arrival_prognosis_status",
        "DURCHFAHRT_TF": "nostopping"
    }
    
    final_df = final_df.rename(columns=column_mapping)
    
    # Generate output filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"historical_data_{timestamp}.csv"
    
    # Save the final DataFrame
    final_df.to_csv(output_file, index=False, sep=",")
    
    print(f"Processing complete. Output saved to {output_file}")
    print(f"Final dataset contains {len(final_df)} rows")
else:
    print("No data matched the filtering criteria")