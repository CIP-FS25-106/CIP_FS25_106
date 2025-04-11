#!/usr/bin/env python
"""
Script to upload a CSV file to Cloudinary.
Usage: python upload_to_cloudinary.py path/to/file.csv.gz
"""

import os
import sys
import cloudinary
import cloudinary.uploader
import cloudinary.api
from dotenv import load_dotenv
import argparse
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json

def setup_resilient_session():
    """Create a requests session with retry logic"""
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def upload_with_retries(file_path, public_id, resource_type='raw', max_attempts=3):
    """
    Attempt to upload a file to Cloudinary with multiple retries
    """
    session = setup_resilient_session()
    cloudinary.config(api_proxy=session)
    
    for attempt in range(1, max_attempts + 1):
        try:
            print(f"Upload attempt {attempt}/{max_attempts}...")
            start_time = time.time()
            
            # Use a longer timeout for large files
            upload_result = cloudinary.uploader.upload(
                file_path,
                resource_type=resource_type,
                public_id=public_id,
                use_filename=True,
                unique_filename=False,
                overwrite=True,
                timeout=300  # 5 minute timeout
            )
            
            end_time = time.time()
            print(f"Upload successful in {end_time - start_time:.2f} seconds!")
            
            return upload_result
            
        except Exception as e:
            print(f"Upload attempt {attempt} failed: {e}")
            if attempt < max_attempts:
                wait_time = 5 * attempt  # Increase wait time with each attempt
                print(f"Waiting {wait_time} seconds before retrying...")
                time.sleep(wait_time)
            else:
                print("Maximum retry attempts reached. Upload failed.")
                raise

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Upload CSV file to Cloudinary')
    parser.add_argument('file_path', help='Path to the CSV file to upload')
    parser.add_argument('--public_id', help='Public ID for the uploaded file (optional)')
    args = parser.parse_args()
    
    # Load environment variables from .env file
    load_dotenv()
    
    # Configure Cloudinary - make sure to use the correct cloud name
    cloud_name = os.getenv('CLOUDINARY_CLOUD_NAME')
    api_key = os.getenv('CLOUDINARY_API_KEY')
    api_secret = os.getenv('CLOUDINARY_API_SECRET')
    
    if not cloud_name or not api_key or not api_secret:
        print("Cloudinary credentials not found in environment variables.")
        print("Make sure you have .env file with CLOUDINARY_CLOUD_NAME, CLOUDINARY_API_KEY, and CLOUDINARY_API_SECRET.")
        sys.exit(1)
    
    print(f"Configuring Cloudinary with cloud_name: {cloud_name}")
    
    cloudinary.config(
        cloud_name=cloud_name,
        api_key=api_key,
        api_secret=api_secret
    )
    
    file_path = args.file_path
    
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} does not exist.")
        sys.exit(1)
    
    # Get file size
    file_size = os.path.getsize(file_path)
    print(f"File size: {file_size:,} bytes ({file_size / (1024 * 1024):.2f} MB)")
    
    # Check if file is too large for Cloudinary free tier
    if file_size > 10 * 1024 * 1024:
        print(f"Warning: File is larger than 10MB, which exceeds Cloudinary's free tier limit.")
        print("The upload will likely fail. Please use a smaller file.")
        response = input("Do you want to continue anyway? (y/n): ")
        if response.lower() != 'y':
            sys.exit(1)
    
    # Set public_id to filename without extension if not provided
    public_id = args.public_id
    if not public_id:
        public_id = os.path.splitext(os.path.basename(file_path))[0]
        # Remove additional extensions like .csv.gz -> just keep the base name
        public_id = os.path.splitext(public_id)[0]
    
    try:
        print(f"Starting upload of {file_path} to Cloudinary as {public_id}...")
        
        # Upload the file with retries
        upload_result = upload_with_retries(file_path, public_id)
        
        if upload_result:
            print("\nUpload successful!")
            print(f"Secure URL: {upload_result['secure_url']}")
            print(f"Public ID: {upload_result['public_id']}")
            
            # Save the URL to the URLs file for later collection
            urls_dir = os.path.dirname(os.path.abspath(__file__))
            urls_file = os.path.join(urls_dir, "cloudinary_urls.txt")
            
            # Append mode to keep all URLs
            with open(urls_file, 'a') as f:
                f.write(f"{upload_result['secure_url']}\n")
            
            print(f"URL appended to {urls_file}")
        
    except Exception as e:
        print(f"Error uploading file to Cloudinary: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()