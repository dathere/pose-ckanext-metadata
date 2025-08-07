#!/usr/bin/env python3
"""
Simple CSV Merger Script
Appends new CSV data to existing CSV data
"""

import pandas as pd
import sys
import os

# Configuration
EXISTING_FILE = 'existing_sites_metadata.csv'
NEW_FILE = 'ckan_stats.csv'
OUTPUT_FILE = 'dynamic_metadata_update.csv'

def merge_csv_files(existing_file, new_file, output_file):
    """
    Simple merge: append new CSV to existing CSV
    
    Args:
        existing_file (str): Path to existing CSV file
        new_file (str): Path to new CSV file
        output_file (str): Path to output merged CSV file
    
    Returns:
        bool: True if successful, False otherwise
    """
    
    print(f"Merging CSV files...")
    print(f"Existing file: {existing_file}")
    print(f"New file: {new_file}")
    print(f"Output file: {output_file}")
    
    try:
        # Check if files exist
        if not os.path.exists(existing_file):
            print(f"✗ Error: {existing_file} not found")
            return False
            
        if not os.path.exists(new_file):
            print(f"✗ Error: {new_file} not found")
            return False
        
        # Read the CSV files
        print("Reading existing metadata...")
        existing_df = pd.read_csv(existing_file)
        print(f"  Rows: {len(existing_df)}")
        
        print("Reading new metadata...")
        new_df = pd.read_csv(new_file)
        print(f"  Rows: {len(new_df)}")
        
        # Simply append new data to existing data
        print("Prepending new data...")
        merged_df = pd.concat([new_df, existing_df], ignore_index=True)
        
        # Save the merged file
        print("Saving merged file...")
        merged_df.to_csv(output_file, index=False)
        
        print(f"✓ Successfully merged files!")
        print(f"  Total rows: {len(merged_df)}")
        print(f"  Output saved to: {output_file}")
        
        return True
        
    except Exception as e:
        print(f"✗ Error merging files: {str(e)}")
        return False

def main():
    """Main function"""
    
    # Allow custom file paths from command line
    existing_file = sys.argv[1] if len(sys.argv) > 1 else EXISTING_FILE
    new_file = sys.argv[2] if len(sys.argv) > 2 else NEW_FILE
    output_file = sys.argv[3] if len(sys.argv) > 3 else OUTPUT_FILE
    
    success = merge_csv_files(existing_file, new_file, output_file)
    
    if success:
        print("Merge complete!")
        sys.exit(0)
    else:
        print("Merge failed!")
        sys.exit(1)

if __name__ == '__main__':
    main()
