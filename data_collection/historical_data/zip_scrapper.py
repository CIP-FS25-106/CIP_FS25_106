from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import requests
import os
import time
from pathlib import Path


# get the path to the project rootdir
project_root = Path(__file__).parent.parent.parent

# I put the chromedriver in a relative folder. The driver is for windows only, for other OS it will need to be adapted
chromedriver_path = project_root / "drivers" / "chromedriver.exe"
service = Service(executable_path=str(chromedriver_path))
driver = webdriver.Chrome(service=service)

# that's the list of years I choose, can be adapted as needed
years = ["2022", "2023", "2024"] 

# loading page we want to scrap
driver.get("https://archive.opentransportdata.swiss/actual_data_archive.htm")
time.sleep(3)  # Wait for JavaScript to load <-- solution found with help

# searching for elements
list_section = driver.find_element(By.ID, "list_dir_section") # list_dir_section is the id of the data_frame I found by inspecting the website
links = list_section.find_elements(By.TAG_NAME, "a") # a is the tag (link)

# process each year
for year in years:
    # set the path for ouput and creates the "year" folder using relative path, if folder already exist = TRUE (else error)
    downloads_dir = project_root / "data" / "historical" / f"downloads_{year}"
    os.makedirs(downloads_dir, exist_ok=True)
        
    # check elements defined before, it has to contain the url and must be a .zip, then it is extracting the filenmane and saves it
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
