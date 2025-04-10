"""
zip_extract_delete.py - Module for extracting downloaded historical train data archives

This module handles extracting downloaded ZIP archives containing historical train data
and removes the archives after successful extraction.
"""

import os
import logging
import patoolib
from pathlib import Path
from typing import List, Set


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("extract_archives.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_project_root() -> Path:
    """
    Get the path to the project root directory.
    
    Returns:
        Path: Project root directory path
    """
    return Path(__file__).parent.parent.parent


def get_target_years() -> List[str]:
    """
    Define target years for processing.
    
    Returns:
        List[str]: Years to process
    """
    return ["2022", "2023", "2024"]


def get_year_folder(project_root: Path, year: str) -> Path:
    """
    Get the folder path for a specific year's downloads.
    
    Args:
        project_root: Path to project root
        year: Year as string
        
    Returns:
        Path: Path to the year's download folder
    """
    return project_root / "data" / "historical" / f"downloads_{year}"


def get_zip_files(folder: Path) -> List[Path]:
    """
    Get all ZIP files in the specified folder.
    
    Args:
        folder: Path to folder containing ZIP files
        
    Returns:
        List[Path]: List of paths to ZIP files
    """
    try:
        # List all files in the folder
        all_files = os.listdir(folder)
        
        # Filter for ZIP files
        zip_files = [folder / file for file in all_files if file.lower().endswith('.zip')]
        
        logger.info(f"Found {len(zip_files)} ZIP files in {folder}")
        return zip_files
    
    except FileNotFoundError:
        logger.error(f"Folder not found: {folder}")
        return []
    except Exception as e:
        logger.error(f"Error listing files in {folder}: {e}")
        return []


def extract_zip_file(zip_path: Path, outdir: Path) -> bool:
    """
    Extract a ZIP file to the specified output directory.
    
    Args:
        zip_path: Path to ZIP file
        outdir: Output directory for extracted files
        
    Returns:
        bool: True if extraction was successful, False otherwise
    """
    try:
        logger.info(f"Extracting: {zip_path}")
        patoolib.extract_archive(str(zip_path), outdir=str(outdir))
        logger.info(f"Successfully extracted: {zip_path}")
        return True
    
    except Exception as e:
        logger.error(f"Error extracting {zip_path}: {e}")
        return False


def delete_zip_file(zip_path: Path) -> bool:
    """
    Delete a ZIP file.
    
    Args:
        zip_path: Path to ZIP file
        
    Returns:
        bool: True if deletion was successful, False otherwise
    """
    try:
        logger.info(f"Deleting: {zip_path}")
        os.remove(zip_path)
        logger.info(f"Successfully deleted: {zip_path}")
        return True
    
    except Exception as e:
        logger.error(f"Error deleting {zip_path}: {e}")
        return False


def process_year_archives(year: str, project_root: Path) -> int:
    """
    Process all ZIP archives for a specific year.
    
    Args:
        year: Year as string
        project_root: Path to project root
        
    Returns:
        int: Number of archives successfully processed
    """
    logger.info(f"Processing archives for year {year}")
    
    # Get the folder for this year
    year_folder = get_year_folder(project_root, year)
    
    if not year_folder.exists():
        logger.warning(f"Folder for year {year} does not exist: {year_folder}")
        return 0
    
    # Get all ZIP files in the folder
    zip_files = get_zip_files(year_folder)
    
    processed_count = 0
    
    # Process each ZIP file
    for zip_path in zip_files:
        try:
            # Extract the ZIP file
            if extract_zip_file(zip_path, year_folder):
                # Delete the ZIP file after successful extraction
                if delete_zip_file(zip_path):
                    processed_count += 1
        
        except Exception as e:
            logger.error(f"Unexpected error processing {zip_path}: {e}")
    
    logger.info(f"Successfully processed {processed_count} out of {len(zip_files)} archives for year {year}")
    return processed_count


def main():
    """Main function to execute the archive extraction process."""
    try:
        # Get project root path
        project_root = get_project_root()
        
        # Get target years
        years = get_target_years()
        
        total_processed = 0
        
        # Process archives for each year
        for year in years:
            year_processed = process_year_archives(year, project_root)
            total_processed += year_processed
        
        logger.info(f"Extraction complete! Successfully processed {total_processed} archives.")
    
    except Exception as e:
        logger.error(f"Unexpected error in main process: {e}")


if __name__ == "__main__":
    main()