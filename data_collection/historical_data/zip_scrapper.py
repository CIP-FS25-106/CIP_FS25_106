from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import requests
import os
import time
from pathlib import Path

# Setup Chrome with relative path to chromedriver
# Get the path to the project root directory
project_root = Path(__file__).parent.parent.parent
chromedriver_path = project_root / "drivers" / "chromedriver.exe"
service = Service(executable_path=str(chromedriver_path))
driver = webdriver.Chrome(service=service)

# List of years to download
years = ["2022", "2023", "2024"]  # Add or remove years as needed

# Load page
driver.get("https://archive.opentransportdata.swiss/actual_data_archive.htm")
time.sleep(3)  # Wait for JavaScript to load

# Find links once
list_section = driver.find_element(By.ID, "list_dir_section")
links = list_section.find_elements(By.TAG_NAME, "a")

# Process each year
for year in years:
    # Create download directory for this year with relative path
    downloads_dir = project_root / "data" / "historical" / f"downloads_{year}"
    os.makedirs(downloads_dir, exist_ok=True)
    
    
    # Filter and download files for this year
    year_files_count = 0
    for link in links:
        href = link.get_attribute('href')
        if href and href.endswith('.zip') and year in href:
            filename = os.path.basename(href)
            save_path = downloads_dir / filename
            
            print(f"Downloading: {filename}")
            file_response = requests.get(href)
            with open(save_path, 'wb') as f:
                f.write(file_response.content)
            print(f"Downloaded {filename}")
            year_files_count += 1


driver.quit()
print("All downloads complete!")
