import os
import patoolib
from pathlib import Path

# get the path to the project rootdir
project_root = Path(__file__).parent.parent.parent

# that's the list of years I choose, can be adapted as needed
years = ["2022", "2023", "2024"]

# we need to process ZIP files for each year
for year in years:
    # use relative path to the downloads folder, with the years defined, make sure the folder exists. 
    folder = project_root / "data" / "historical" / f"downloads_{year}"
    
    print(f"Processing ZIP files in: {folder}")
    
    # list all files in the folder
    files = os.listdir(folder)
    
    # process each ZIP file <-- had to do research for that
    for file in files:
        if file.lower().endswith('.zip'):
            zip_path = folder / file
            try:
                # with patool you can extact the zip file
                patoolib.extract_archive(str(zip_path), outdir=str(folder))
                
                # this is deleting the zip file after extraction to 
                os.remove(zip_path)
                print(f"Extracted and deleted: {zip_path}")

                # catch the error, so if one file is not working, it is not aborting
            except Exception as e:
                print(f"Error with {zip_path}: {e}")

print("Extraction complete!")