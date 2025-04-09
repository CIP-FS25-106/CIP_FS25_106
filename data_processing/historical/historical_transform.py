import pandas as pd
import numpy as np
from pathlib import Path

# get the path to the project rootdir
project_root = Path(__file__).parent.parent.parent

# input file and DF creation
input_file = project_root / "data" / "historical" / "processed" / "intermediate_filtered_data.csv"
df = pd.read_csv(input_file)

# convert to datetime using mixed format. it's solution with missing values and because format is not exactly the same
df["ANKUNFTSZEIT_DT"] = pd.to_datetime(df["ANKUNFTSZEIT"], format="mixed", dayfirst=True, errors="coerce")
df["AN_PROGNOSE_DT"] = pd.to_datetime(df["AN_PROGNOSE"], format="mixed", dayfirst=True, errors="coerce")

# compute delay in minutes
df["DELAY"] = (df["AN_PROGNOSE_DT"] - df["ANKUNFTSZEIT_DT"]).dt.total_seconds() / 60

# add a new column DELAY CATEGORY
conditions = [
    df["FAELLT_AUS_TF"] == True,
    (df["DELAY"] > 2) & (df["DELAY"] <= 5),
    (df["DELAY"] > 5) & (df["DELAY"] <= 15),
    df["DELAY"] > 15
]
choices = ["Cancelled", "2 to 5minutes", "5 to 15minutes", "more than 15minutes"]
df["DELAY_CAT"] = np.select(conditions, choices, default="On time")

# rename columns, so it matches the column names from the API (easier to concatenate later)
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
    "ANKUNFTSZEIT_DT": "scheduled_arrival",
    "AN_PROGNOSE_DT": "arrival_prognosis",
    "AN_PROGNOSE_STATUS": "arrival_prognosis_status",
    "DURCHFAHRT_TF": "nostopping"
}
df = df.rename(columns=column_mapping)

# drop the column that is not needed anymore
df = df.drop(columns=["ANKUNFTSZEIT", "AN_PROGNOSE"])

# save to output
output_dir = project_root / "data" / "historical" / "processed"
output_file = output_dir / "historical_transformed.csv"
df.to_csv(output_file, index=False, sep=",")