"""
zip_scrapper.py - Module for downloading historical train data archives

This module handles the web scraping and downloading of historical train data
archives from the OpenTransportData Swiss website for specified years.
"""

import os
import time
import logging
import requests
from pathlib import Path
from typing import List, Optional

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, WebDriverException


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("download_historical.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# Constants
ARCHIVE_URL = "https://archive.opentransportdata.swiss/actual_data_archive.htm"
PAGE_LOAD_WAIT_TIME = 3  # Seconds to wait for JavaScript to load


def get_project_root() -> Path:
    """
    Get the path to the project root directory.
    
    Returns:
        Path: Project root directory path
    """
    return Path(__file__).parent.parent.parent


def get_chromedriver_path(project_root: Path) -> Path:
    """
    Get the path to the Chrome WebDriver executable.
    
    Args:
        project_root: Path to project root directory
        
    Returns:
        Path: Path to chromedriver executable
    """
    return project_root / "drivers" / "chromedriver.exe"


def initialize_webdriver(chromedriver_path: Path) -> Optional[webdriver.Chrome]:
    """
    Initialize and return a Chrome WebDriver.
    
    Args:
        chromedriver_path: Path to chromedriver executable
        
    Returns:
        webdriver.Chrome or None: Initialized WebDriver or None if initialization fails
    """
    try:
        service = Service(executable_path=str(chromedriver_path))
        driver = webdriver.Chrome(service=service)
        logger.info("WebDriver initialized successfully")
        return driver
    except WebDriverException as e:
        logger.error(f"Error initializing WebDriver: {e}")
        return None


def load_archive_page(driver: webdriver.Chrome) -> bool:
    """
    Load the archive page and wait for JavaScript to render.
    
    Args:
        driver: Chrome WebDriver instance
        
    Returns:
        bool: True if page loaded successfully, False otherwise
    """
    try:
        logger.info(f"Loading archive page: {ARCHIVE_URL}")
        driver.get(ARCHIVE_URL)
        time.sleep(PAGE_LOAD_WAIT_TIME)  # Wait for JavaScript to load
        return True
    except WebDriverException as e:
        logger.error(f"Error loading archive page: {e}")
        return False


def find_archive_links(driver: webdriver.Chrome) -> List:
    """
    Find all archive links on the page.
    
    Args:
        driver: Chrome WebDriver instance
        
    Returns:
        List: List of WebElement links
    """
    try:
        logger.info("Searching for archive links")
        list_section = driver.find_element(By.ID, "list_dir_section")
        links = list_section.find_elements(By.TAG_NAME, "a")
        logger.info(f"Found {len(links)} total links")
        return links
    except NoSuchElementException as e:
        logger.error(f"Error finding archive links: {e}")
        return []


def ensure_download_directory(project_root: Path, year: str) -> Path:
    """
    Ensure the download directory for the specified year exists.
    
    Args:
        project_root: Path to project root
        year: Year as string
        
    Returns:
        Path: Path to download directory
    """
    downloads_dir = project_root / "data" / "historical" / f"downloads_{year}"
    os.makedirs(downloads_dir, exist_ok=True)
    logger.info(f"Ensured download directory exists: {downloads_dir}")
    return downloads_dir


def download_file(url: str, save_path: Path) -> bool:
    """
    Download a file from the specified URL and save it to the specified path.
    
    Args:
        url: URL of the file to download
        save_path: Path where the file should be saved
        
    Returns:
        bool: True if download was successful, False otherwise
    """
    try:
        logger.info(f"Downloading: {url}")
        file_response = requests.get(url)
        file_response.raise_for_status()  # Raise exception for 4XX/5XX responses
        
        with open(save_path, 'wb') as f:
            f.write(file_response.content)
            
        logger.info(f"Downloaded successfully: {save_path}")
        return True
    except (requests.RequestException, IOError) as e:
        logger.error(f"Error downloading {url}: {e}")
        return False


def download_archives_for_year(links: List, downloads_dir: Path, year: str) -> int:
    """
    Download all archive files for the specified year.
    
    Args:
        links: List of WebElement links
        downloads_dir: Path to download directory
        year: Year as string
        
    Returns:
        int: Number of files downloaded
    """
    logger.info(f"Processing archives for year {year}")
    year_files_count = 0
    
    for link in links:
        try:
            href = link.get_attribute('href')
            if href and href.endswith('.zip') and year in href:
                filename = os.path.basename(href)
                save_path = downloads_dir / filename
                
                if save_path.exists():
                    logger.info(f"File already exists, skipping: {filename}")
                    year_files_count += 1
                    continue
                
                if download_file(href, save_path):
                    year_files_count += 1
        except Exception as e:
            logger.error(f"Error processing link: {e}")
    
    logger.info(f"Downloaded {year_files_count} files for year {year}")
    return year_files_count


def main():
    """Main function to execute the download process."""
    try:
        # Get project root path
        project_root = get_project_root()
        
        # Get chromedriver path
        chromedriver_path = get_chromedriver_path(project_root)
        
        # Initialize webdriver
        driver = initialize_webdriver(chromedriver_path)
        if not driver:
            logger.error("Failed to initialize WebDriver. Exiting.")
            return
        
        # Load archive page
        if not load_archive_page(driver):
            logger.error("Failed to load archive page. Exiting.")
            driver.quit()
            return
        
        # Find archive links
        links = find_archive_links(driver)
        if not links:
            logger.error("No archive links found. Exiting.")
            driver.quit()
            return
        
        # Define years to process
        years = ["2022", "2023", "2024"]
        total_files_count = 0
        
        # Process each year
        for year in years:
            # Ensure download directory exists
            downloads_dir = ensure_download_directory(project_root, year)
            
            # Download archives for the year
            year_files_count = download_archives_for_year(links, downloads_dir, year)
            total_files_count += year_files_count
        
        logger.info(f"All downloads complete! Total files: {total_files_count}")
    
    except Exception as e:
        logger.error(f"Unexpected error in main process: {e}")
    
    finally:
        # Clean up
        if 'driver' in locals() and driver:
            driver.quit()
            logger.info("WebDriver closed")


if __name__ == "__main__":
    main()