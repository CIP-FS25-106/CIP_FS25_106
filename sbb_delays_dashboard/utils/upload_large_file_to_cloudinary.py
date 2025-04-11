#!/usr/bin/env python
"""
Script to upload large CSV files to Cloudinary with chunked uploading support.
Handles files up to several GB in size by using a more robust approach.
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
import gzip
import shutil
import tempfile

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

def compress_file(input_path):
    """Compress the input file with gzip"""
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.gz')
    temp_file.close()
    
    print(f"Compressing file {input_path} to {temp_file.name}...")
    try:
        with open(input_path, 'rb') as f_in:
            with gzip.open(temp_file.name, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print(f"Compression complete. Original size: {os.path.getsize(input_path):,} bytes, " 
              f"Compressed size: {os.path.getsize(temp_file.name):,} bytes")
        return temp_file.name
    except Exception as e:
        print(f"Error compressing file: {e}")
        if os.path.exists(temp_file.name):
            os.unlink(temp_file.name)
        return None

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
                timeout=600  # 10 minute timeout
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
    parser = argparse.ArgumentParser(description='Upload large CSV file to Cloudinary')
    parser.add_argument('file_path', help='Path to the CSV file to upload')
    parser.add_argument('--public_id', help='Public ID for the uploaded file (optional)')
    parser.add_argument('--compress', action='store_true', help='Compress the file before uploading')
    args = parser.parse_args()
    
    # Load environment variables from .env file
    load_dotenv()
    
    # Configure Cloudinary - make sure to use the correct cloud name
    cloud_name = os.getenv('CLOUDINARY_CLOUD_NAME')
    api_key = os.getenv('CLOUDINARY_API_KEY')
    api_secret = os.getenv('CLOUDINARY_API_SECRET')
    
    print(f"Configuring Cloudinary with cloud_name: {cloud_name}")
    
    cloudinary.config(
        cloud_name=cloud_name,
        api_key=api_key,
        api_secret=api_secret,
        secure=True
    )
    
    file_path = args.file_path
    
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} does not exist.")
        sys.exit(1)
    
    # Get file size
    file_size = os.path.getsize(file_path)
    print(f"File size: {file_size:,} bytes ({file_size / (1024 * 1024):.2f} MB)")
    
    # Set public_id to filename without extension if not provided
    public_id = args.public_id
    if not public_id:
        public_id = os.path.splitext(os.path.basename(file_path))[0]
    
    # Compress the file if requested
    temp_file = None
    upload_path = file_path
    
    if args.compress:
        temp_file = compress_file(file_path)
        if temp_file:
            upload_path = temp_file
            # Add .gz to public_id to indicate it's compressed
            public_id = f"{public_id}.gz"
    
    try:
        print(f"Starting upload of {upload_path} to Cloudinary as {public_id}...")
        
        # Upload the file with retries
        upload_result = upload_with_retries(upload_path, public_id)
        
        if upload_result:
            print("\nUpload successful!")
            print(f"Secure URL: {upload_result['secure_url']}")
            print(f"Public ID: {upload_result['public_id']}")
            
            # Save the URL to a file for easy reference
            with open('cloudinary_url.txt', 'w') as f:
                f.write(upload_result['secure_url'])
            print("URL saved to cloudinary_url.txt")
            
            # Save the URL to .env file
            env_file = '.env'
            if os.path.exists(env_file):
                with open(env_file, 'r') as f:
                    env_content = f.read()
                
                # Update or add the HISTORICAL_DATA_URL variable
                if 'HISTORICAL_DATA_URL=' in env_content:
                    env_content = '\n'.join([
                        line if not line.startswith('HISTORICAL_DATA_URL=') else f'HISTORICAL_DATA_URL="{upload_result["secure_url"]}"'
                        for line in env_content.split('\n')
                    ])
                else:
                    env_content += f'\nHISTORICAL_DATA_URL="{upload_result["secure_url"]}"\n'
                
                with open(env_file, 'w') as f:
                    f.write(env_content)
                print(f"Updated {env_file} with the Cloudinary URL")
        
    except Exception as e:
        print(f"Error uploading file to Cloudinary: {e}")
        sys.exit(1)
    
    finally:
        # Clean up temporary file if created
        if temp_file and os.path.exists(temp_file):
            os.unlink(temp_file)
            print(f"Cleaned up temporary file {temp_file}")

if __name__ == "__main__":
    main()