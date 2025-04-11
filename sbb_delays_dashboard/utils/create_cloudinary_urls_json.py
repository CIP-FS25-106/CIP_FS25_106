#!/usr/bin/env python
"""
Script to create a JSON file with all Cloudinary URLs.
Usage: python create_cloudinary_urls_json.py
"""

import os
import json
import argparse

def create_urls_json(urls_txt_file, output_json_file, env_file=None):
    """
    Read URLs from a text file and create a JSON file
    
    Args:
        urls_txt_file: Text file with one URL per line
        output_json_file: JSON file to create
        env_file: Optional .env file to update
    """
    # Check if input file exists
    if not os.path.exists(urls_txt_file):
        print(f"Error: File {urls_txt_file} does not exist.")
        return False
    
    # Read URLs from text file
    with open(urls_txt_file, 'r') as f:
        urls = [line.strip() for line in f.readlines() if line.strip()]
    
    # Sort URLs to ensure consistent order
    urls.sort()
    
    # Remove duplicates
    urls = list(dict.fromkeys(urls))
    
    # Write URLs to JSON file
    with open(output_json_file, 'w') as f:
        json.dump(urls, f, indent=2)
    
    print(f"Created JSON file with {len(urls)} URLs: {output_json_file}")
    
    # Update .env file if specified
    if env_file:
        update_env_file(env_file, urls)
    
    return True

def update_env_file(env_file, urls):
    """
    Update the .env file with the Cloudinary URLs
    """
    # Check if .env file exists
    if not os.path.exists(env_file):
        # Create new .env file
        with open(env_file, 'w') as f:
            f.write(f'HISTORICAL_DATA_URLS=\'{json.dumps(urls)}\'\n')
        print(f"Created new .env file: {env_file}")
        return True
    
    # Read existing .env file
    with open(env_file, 'r') as f:
        env_content = f.read()
    
    # Create JSON string from URLs
    urls_json = json.dumps(urls)
    
    # Update or add HISTORICAL_DATA_URLS
    if 'HISTORICAL_DATA_URLS=' in env_content:
        # Update existing variable
        lines = env_content.split('\n')
        updated_lines = []
        for line in lines:
            if line.startswith('HISTORICAL_DATA_URLS='):
                updated_lines.append(f'HISTORICAL_DATA_URLS=\'{urls_json}\'')
            else:
                updated_lines.append(line)
        
        updated_content = '\n'.join(updated_lines)
    else:
        # Add new variable
        updated_content = env_content.rstrip() + f'\n\nHISTORICAL_DATA_URLS=\'{urls_json}\'\n'
    
    # Write updated content back to .env file
    with open(env_file, 'w') as f:
        f.write(updated_content)
    
    print(f"Updated {env_file} with {len(urls)} Cloudinary URLs")
    return True

def main():
    parser = argparse.ArgumentParser(description='Create JSON file with Cloudinary URLs')
    parser.add_argument('--input', default='cloudinary_urls.txt', help='Text file with URLs (one per line)')
    parser.add_argument('--output', default='cloudinary_urls.json', help='Output JSON file')
    parser.add_argument('--env', help='Update .env file with URLs')
    
    args = parser.parse_args()
    
    try:
        success = create_urls_json(args.input, args.output, args.env)
        
        if success:
            # Print instructions
            print("\nNext steps:")
            print("1. Add the following line to your app.py:")
            print("   cloudinary_urls_file = 'cloudinary_urls.json'")
            print("2. Update your load_historical_data call:")
            print("   df = load_historical_data(urls_file=cloudinary_urls_file)")
            
            if args.env:
                print(f"3. Deploy with the updated {args.env} file")
                print("   The HISTORICAL_DATA_URLS environment variable is now set.")
            else:
                print("3. Set the HISTORICAL_DATA_URLS environment variable for deployment")
        
    except Exception as e:
        print(f"Error creating JSON file: {e}")

if __name__ == "__main__":
    main()