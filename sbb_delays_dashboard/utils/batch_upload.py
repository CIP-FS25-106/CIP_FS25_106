#!/usr/bin/env python
"""
Script to batch upload all CSV chunks to Cloudinary.
Usage: python batch_upload.py --directory csv_chunks
"""

import os
import sys
import argparse
import subprocess
import time
import glob

def batch_upload(directory, upload_script):
    """
    Upload all CSV files in a directory to Cloudinary
    
    Args:
        directory: Directory containing CSV files
        upload_script: Path to the upload script
    """
    # Check if directory exists
    if not os.path.exists(directory):
        print(f"Error: Directory {directory} does not exist.")
        return False
    
    # Check if upload script exists
    if not os.path.exists(upload_script):
        print(f"Error: Upload script {upload_script} does not exist.")
        return False
    
    # Get all CSV files in the directory
    file_pattern = os.path.join(directory, "*.csv*")
    files = sorted(glob.glob(file_pattern))
    
    if not files:
        print(f"No CSV files found in {directory}")
        return False
    
    print(f"Found {len(files)} files to upload:")
    for file in files:
        print(f"  - {os.path.basename(file)}")
    
    # Confirm with user
    response = input(f"\nDo you want to upload these {len(files)} files to Cloudinary? (y/n): ")
    if response.lower() != 'y':
        print("Upload cancelled.")
        return False
    
    # Track uploads
    successful = []
    failed = []
    
    # Upload each file
    for i, file_path in enumerate(files):
        print(f"\nUploading file {i+1}/{len(files)}: {os.path.basename(file_path)}")
        
        try:
            # Run the upload script
            cmd = [sys.executable, upload_script, file_path]
            result = subprocess.run(cmd, check=True)
            
            if result.returncode == 0:
                successful.append(file_path)
                print(f"Successfully uploaded {os.path.basename(file_path)}")
            else:
                failed.append(file_path)
                print(f"Upload failed for {os.path.basename(file_path)}")
            
            # Wait between uploads to avoid rate limits
            if i < len(files) - 1:
                print("Waiting 5 seconds before next upload...")
                time.sleep(5)
                
        except subprocess.CalledProcessError as e:
            failed.append(file_path)
            print(f"Error uploading {os.path.basename(file_path)}: {e}")
            
            # Ask user if they want to continue
            if len(failed) > 2:
                response = input("Multiple uploads failed. Continue with remaining files? (y/n): ")
                if response.lower() != 'y':
                    print("Batch upload cancelled.")
                    break
    
    # Print summary
    print("\n==== Upload Summary ====")
    print(f"Total files: {len(files)}")
    print(f"Successfully uploaded: {len(successful)}")
    print(f"Failed: {len(failed)}")
    
    if failed:
        print("\nFailed files:")
        for f in failed:
            print(f"  - {os.path.basename(f)}")
        
        print("\nYou may try uploading the failed files again with:")
        for f in failed:
            print(f"python {upload_script} {f}")
    
    return len(failed) == 0

def main():
    parser = argparse.ArgumentParser(description='Batch upload CSV files to Cloudinary')
    parser.add_argument('--directory', default='csv_chunks', help='Directory containing CSV files')
    parser.add_argument('--upload-script', default='upload_to_cloudinary.py', help='Path to upload script')
    
    args = parser.parse_args()
    
    try:
        success = batch_upload(args.directory, args.upload_script)
        
        if success:
            print("\nAll files uploaded successfully!")
            print("Next step: Run create_cloudinary_urls_json.py to create a JSON file with all URLs")
        else:
            print("\nSome files failed to upload. Please check the error messages and try again.")
        
    except Exception as e:
        print(f"Error in batch upload: {e}")

if __name__ == "__main__":
    main()