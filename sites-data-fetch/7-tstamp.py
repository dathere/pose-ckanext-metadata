#!/usr/bin/env python3
"""
CSV Timestamp Script
Adds a UTC timestamp column to a CSV file
"""

import csv
import datetime
import os
import sys
from typing import Optional


def add_timestamp_to_csv(input_file: str, output_file: Optional[str] = None, 
                        timestamp_column: str = 'tstamp', 
                        timestamp_format: str = 'date') -> None:
    """
    Add UTC timestamp to all rows in a CSV file
    
    Args:
        input_file: Path to input CSV file
        output_file: Path to output CSV file (if None, overwrites input file)
        timestamp_column: Name of the timestamp column to add
        timestamp_format: Format of timestamp ('date', 'iso', 'epoch', 'readable')
    """
    
    # Generate timestamp
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    
    # Format timestamp based on requested format
    if timestamp_format == 'date':
        # Date format: 2025-07-18
        timestamp = now_utc.strftime('%Y-%m-%d')
    elif timestamp_format == 'iso':
        # ISO 8601 format: 2025-07-02T17:45:30.123456+00:00
        timestamp = now_utc.isoformat()
    elif timestamp_format == 'epoch':
        # Unix epoch timestamp: 1719940530.123456
        timestamp = str(now_utc.timestamp())
    elif timestamp_format == 'readable':
        # Human readable format: 2025-07-02 17:45:30 UTC
        timestamp = now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')
    else:
        raise ValueError(f"Invalid timestamp format: {timestamp_format}. Use 'date', 'iso', 'epoch', or 'readable'")
    
    # If no output file specified, create a timestamped version
    if output_file is None:
        base_name = os.path.splitext(input_file)[0]
        extension = os.path.splitext(input_file)[1]
        output_file = f"{base_name}_timestamped{extension}"
    
    print(f"Processing: {input_file}")
    print(f"Output: {output_file}")
    print(f"Timestamp: {timestamp}")
    print(f"Timestamp column: {timestamp_column}")
    
    try:
        # Read input CSV
        with open(input_file, 'r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            
            # Check if fieldnames exist
            if reader.fieldnames is None:
                raise ValueError(f"CSV file '{input_file}' appears to be empty or has no header row")
            
            # Create new fieldnames with timestamp column first
            original_fieldnames = list(reader.fieldnames)
            
            # Remove timestamp column if it already exists in original fieldnames
            if timestamp_column in original_fieldnames:
                original_fieldnames.remove(timestamp_column)
                print(f"Column '{timestamp_column}' already exists, will be overwritten and moved to first position")
            else:
                print(f"Adding new column: {timestamp_column} (as first column)")
            
            # Create fieldnames with timestamp column first
            fieldnames = [timestamp_column] + original_fieldnames
            
            # Read all rows and add timestamp
            rows = []
            for row_num, row in enumerate(reader, 1):
                row[timestamp_column] = timestamp
                rows.append(row)
            
            print(f"Processed {len(rows)} rows")
        
        # Write output CSV
        with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        
        print(f"Successfully created timestamped CSV: {output_file}")
        
    except FileNotFoundError:
        print(f"ERROR: Input file '{input_file}' not found!")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: {str(e)}")
        sys.exit(1)


def main():
    """Main function with command line argument handling"""
    
    # Default configuration
    INPUT_FILE = '6.csv'  # Change this to your input file
    OUTPUT_FILE = '7.csv'    # None means auto-generate name
    TIMESTAMP_COLUMN = 'tstamp'
    TIMESTAMP_FORMAT = 'date'  # Changed to 'date' for YYYY-MM-DD format
    
    # Simple command line argument handling
    if len(sys.argv) > 1:
        INPUT_FILE = sys.argv[1]
    if len(sys.argv) > 2:
        OUTPUT_FILE = sys.argv[2]
    
    print("CSV Timestamp Script")
    print("=" * 50)
    print(f"Input file: {INPUT_FILE}")
    print(f"Output file: {OUTPUT_FILE or 'Auto-generated'}")
    print(f"Timestamp format: {TIMESTAMP_FORMAT}")
    print()
    
    # Check if input file exists
    if not os.path.exists(INPUT_FILE):
        print(f"ERROR: Input file '{INPUT_FILE}' not found!")
        print("Usage:")
        print(f"  python {sys.argv[0]} <input_file> [output_file]")
        print()
        print("Examples:")
        print(f"  python {sys.argv[0]} data.csv")
        print(f"  python {sys.argv[0]} data.csv data_with_timestamp.csv")
        return
    
    # Process the file
    add_timestamp_to_csv(
        input_file=INPUT_FILE,
        output_file=OUTPUT_FILE,
        timestamp_column=TIMESTAMP_COLUMN,
        timestamp_format=TIMESTAMP_FORMAT
    )


def demo_timestamp_formats():
    """Show examples of different timestamp formats"""
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    
    print("Timestamp Format Examples:")
    print("=" * 40)
    print(f"Date format:     {now_utc.strftime('%Y-%m-%d')}")
    print(f"ISO format:      {now_utc.isoformat()}")
    print(f"Epoch format:    {now_utc.timestamp()}")
    print(f"Readable format: {now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print()


if __name__ == "__main__":
    # Uncomment the line below to see timestamp format examples
    # demo_timestamp_formats()
    
    main()
