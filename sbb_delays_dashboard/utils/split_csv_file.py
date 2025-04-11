#!/usr/bin/env python
"""
Script to split a large CSV file into smaller chunks under 10MB for Cloudinary upload.
Fixed version to handle permission issues.
Usage: python split_csv_file_fixed.py path/to/large_file.csv
"""

import os
import sys
import pandas as pd
import argparse
import math
from pathlib import Path
import tempfile
import gzip
import shutil

def estimate_compressed_size(df, temp_dir):
    """Estimate compressed size of a DataFrame by writing to a temporary gzipped file"""
    # Create a temporary file in the specified directory
    temp_file_path = os.path.join(temp_dir, f"temp_estimate_{os.getpid()}.csv.gz")
    try:
        df.to_csv(temp_file_path, index=False, compression='gzip')
        size = os.path.getsize(temp_file_path)
        return size
    finally:
        # Clean up the temporary file
        if os.path.exists(temp_file_path):
            try:
                os.remove(temp_file_path)
            except:
                pass

def split_csv_by_size(input_file, output_dir, temp_dir, target_size_mb=8.5):
    """
    Split a CSV file into multiple files with each compressed file size under target_size_mb.
    
    Args:
        input_file: Path to the input CSV file
        output_dir: Directory to save the output files
        temp_dir: Directory for temporary files
        target_size_mb: Target size in MB for each output file (after compression)
    
    Returns:
        List of created file paths
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Create temp directory if it doesn't exist
    os.makedirs(temp_dir, exist_ok=True)
    
    # Get base filename without extension
    base_filename = os.path.splitext(os.path.basename(input_file))[0]
    
    # Get input file size
    input_file_size = os.path.getsize(input_file) / (1024 * 1024)  # Size in MB
    print(f"Input file: {input_file}")
    print(f"Input file size: {input_file_size:.2f} MB")
    
    # Target size in bytes
    target_size_bytes = target_size_mb * 1024 * 1024
    
    # Check total rows
    print("Counting total rows (this may take a while for large files)...")
    try:
        total_rows = sum(1 for _ in open(input_file, 'r', encoding='utf-8')) - 1  # Subtract header
    except UnicodeDecodeError:
        # Try Latin-1 encoding if UTF-8 fails
        total_rows = sum(1 for _ in open(input_file, 'r', encoding='latin-1')) - 1
    
    print(f"Total rows: {total_rows:,}")
    
    # Start with a conservative chunk size
    rows_per_chunk = 50000  # Initial guess
    
    # Read the header and first chunk to estimate size
    try:
        df_sample = pd.read_csv(input_file, nrows=rows_per_chunk)
    except UnicodeDecodeError:
        # Try with Latin-1 encoding if UTF-8 fails
        df_sample = pd.read_csv(input_file, nrows=rows_per_chunk, encoding='latin-1')
    
    # Estimate compressed size
    sample_compressed_size = estimate_compressed_size(df_sample, temp_dir)
    
    # Calculate better rows_per_chunk based on sample
    compression_ratio = len(df_sample) / (sample_compressed_size / target_size_bytes)
    rows_per_chunk = int(compression_ratio * 0.9)  # 10% safety margin
    
    print(f"Compression ratio: {compression_ratio:.2f}")
    print(f"Using {rows_per_chunk:,} rows per chunk")
    
    # Calculate number of chunks
    num_chunks = math.ceil(total_rows / rows_per_chunk)
    print(f"Estimated number of chunks: {num_chunks}")
    
    # Process in chunks
    output_files = []
    
    # Determine appropriate encoding
    try:
        pd.read_csv(input_file, nrows=5)
        encoding = 'utf-8'
    except UnicodeDecodeError:
        encoding = 'latin-1'
    
    print(f"Using {encoding} encoding")
    
    # Read the CSV in chunks and write to separate files
    chunk_reader = pd.read_csv(input_file, chunksize=rows_per_chunk, encoding=encoding)
    
    for i, chunk in enumerate(chunk_reader):
        print(f"Processing chunk {i+1}/{num_chunks}...")
        
        # Generate output filename
        output_file = os.path.join(output_dir, f"{base_filename}_part{i+1:03d}.csv.gz")
        
        # Compress and save the chunk
        chunk.to_csv(output_file, index=False, compression='gzip')
        
        # Check file size
        file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
        print(f"Created {output_file} ({file_size_mb:.2f} MB) with {len(chunk):,} rows")
        
        # Adjust rows_per_chunk if necessary
        if i == 0 and file_size_mb > target_size_mb:
            # First chunk too large, reduce rows for subsequent chunks
            adjustment_factor = target_size_mb / file_size_mb * 0.9  # 10% safety margin
            rows_per_chunk = int(rows_per_chunk * adjustment_factor)
            print(f"Adjusted rows per chunk to {rows_per_chunk:,}")
        
        output_files.append(output_file)
    
    print(f"\nCreated {len(output_files)} files in {output_dir}")
    print(f"Total output size: {sum(os.path.getsize(f) for f in output_files) / (1024 * 1024):.2f} MB")
    
    return output_files

def main():
    parser = argparse.ArgumentParser(description='Split a large CSV file into smaller chunks for Cloudinary upload')
    parser.add_argument('input_file', help='Path to the input CSV file')
    parser.add_argument('--output-dir', help='Directory to save the output files', default='csv_chunks')
    parser.add_argument('--temp-dir', help='Directory for temporary files', default='temp_csv')
    parser.add_argument('--target-size', type=float, help='Target size in MB for each output file', default=8.5)
    
    args = parser.parse_args()
    
    try:
        # Make sure the temp directory exists
        os.makedirs(args.temp_dir, exist_ok=True)
        
        output_files = split_csv_by_size(
            args.input_file, 
            args.output_dir,
            args.temp_dir,
            args.target_size
        )
        
        print("\nNext steps:")
        print("1. Upload each chunk to Cloudinary using upload_to_cloudinary.py:")
        print("   python upload_to_cloudinary.py path/to/chunk.csv.gz")
        print("2. Create a JSON file with all URLs using create_cloudinary_urls_json.py")
        print("3. Update your application's .env file with HISTORICAL_DATA_URLS")
        
    except Exception as e:
        print(f"Error splitting CSV file: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        # Clean up temp directory if it's empty
        try:
            if os.path.exists(args.temp_dir) and not os.listdir(args.temp_dir):
                os.rmdir(args.temp_dir)
        except:
            pass

if __name__ == "__main__":
    main()