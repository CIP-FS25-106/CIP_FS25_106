import pandas as pd
import os
import glob
from pathlib import Path

# get the path to the project rootdir
project_root = Path(__file__).parent.parent.parent

# here we can define the station to filter for
stations = ["Zürich HB", "Luzern", "Genève"]

# those columns will be kept
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

# this will create the new folder for the intermediate df csv
output_dir = project_root / "data" / "historical" / "processed"
os.makedirs(output_dir, exist_ok=True)

# those are the years we are including
years = ["2022", "2023", "2024"]

# initialize an empty list to store filtered DataFrames <-- had to do some research to find this idea of processing csv's
all_filtered_chunks = []

# loop for processing each year folder
for year in years:
    # and so this is the path to dat folder for the year
    folder_path = project_root / "data" / "historical" / f"downloads_{year}"
        
    print(f"Processing dat for year {year}...")
    
    # and this gets all csv files in the folder for the year
    csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
    
    # second loop is to process each csv file from the folder
    for file_path in csv_files:
        print(f"Processing file: {os.path.basename(file_path)}")
        
        # create a CSV reader that processes parts <-- had to do some research to find this idea of processing csv's
        csv_reader = pd.read_csv(
            file_path, 
            sep=";",
            chunksize=100000, # is needed to define partsizem, can be opitmized for large files
            usecols=columns_to_keep,
            low_memory=False
        )
        
        # and here it's applying the filtering and the appending for each chunk
        for i, chunk in enumerate(csv_reader):
            # first filter
            filtered_chunk = chunk[
                (chunk["BETREIBER_ABK"] == "SBB") &
                (chunk["PRODUKT_ID"] == "Zug") &
                (chunk["ZUSATZFAHRT_TF"] == False) &
                (chunk["DURCHFAHRT_TF"] == False) &
                (chunk["HALTESTELLEN_NAME"].isin(stations)) &
                (chunk["AN_PROGNOSE_STATUS"].notna())
            ]
            
            # and then appending
            all_filtered_chunks.append(filtered_chunk)
        
        print(f"Finished processing {os.path.basename(file_path)}")

# last is concatenating all parts with panda, and saving it to the output filder as a csv file
combined = pd.concat(all_filtered_chunks, ignore_index=True)
intermediate_file = output_dir / "intermediate_filtered_data.csv"
combined.to_csv(intermediate_file, index=False, sep=",")
print(f"Filtered data saved to {intermediate_file}")
