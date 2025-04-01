import os
import patoolib
from pathlib import Path

# Get the path to the project root directory
project_root = Path(__file__).parent.parent.parent

# List of years to process
years = ["2022", "2023", "2024"]

# Process ZIP files for each year
for year in years:
    # Use relative path to the downloads folder
    folder = project_root / "data" / "historical" / f"downloads_{year}"
    
    # Skip if folder doesn't exist
    if not folder.exists():
        print(f"Folder not found: {folder}")
        continue
    
    print(f"Processing ZIP files in: {folder}")
    
    # List all files in the folder
    files = os.listdir(folder)
    
    # Process each ZIP file
    for file in files:
        if file.lower().endswith('.zip'):
            zip_path = folder / file
            try:
                # Extract the ZIP file using patool
                patoolib.extract_archive(str(zip_path), outdir=str(folder))
                
                # Delete the ZIP file
                os.remove(zip_path)
                print(f"Extracted and deleted: {zip_path}")
            except Exception as e:
                print(f"Error with {zip_path}: {e}")

print("Extraction complete!")