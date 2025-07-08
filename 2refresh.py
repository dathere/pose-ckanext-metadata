#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import pandas as pd
from github import Github, RateLimitExceededException, GithubException
from datetime import datetime, timedelta
import time
import traceback
from typing import Dict, Optional, List

# Configuration
REPO_LIMIT = 10000  # Adjust as needed
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN', 'GH_METADATA_TOKEN')

class DynamicMetadataUpdater:
    def __init__(self, github_token: str):
        """Initialize the updater with GitHub token"""
        self.github = Github(github_token)
        self.processed_count = 0
        self.error_count = 0
        self.start_time = datetime.now()

    def print_status(self, current, total):
        """Print status with ETA"""
        if current == 0:
            self.start_time = datetime.now()
            print(f"Processing {current}/{total} repositories")
            return
            
        elapsed = (datetime.now() - self.start_time).total_seconds()
        rate = elapsed / current if current > 0 else 0
        remaining = rate * (total - current)
        
        # Format as hours:minutes:seconds
        eta = str(timedelta(seconds=int(remaining)))
        
        print(f"Processing {current}/{total} repositories | " +
            f"Elapsed: {timedelta(seconds=int(elapsed))} | " +
            f"ETA: {eta} | " +
            f"Success: {self.processed_count} | " +
            f"Errors: {self.error_count}")

    def check_discussions_enabled(self, repo) -> bool:
        """
        Check if GitHub Discussions are enabled for the repository
        
        Args:
            repo: The GitHub repository object
            
        Returns:
            bool: True if discussions are enabled, False otherwise
        """
        try:
            # Try to access discussions - if it fails, discussions are likely disabled
            discussions = list(repo.get_discussions())
            return True
        except Exception:
            # If we can't access discussions, they're either disabled or we don't have permission
            # Try alternative method using repository attributes
            try:
                # Check if the repository has discussions enabled via the API
                # This is a more reliable method
                return repo.has_discussions
            except AttributeError:
                # Fallback: try to make a request to the discussions API endpoint
                try:
                    url = f"https://api.github.com/repos/{repo.full_name}/discussions"
                    response = self.github._Github__requester.requestJsonAndCheck("GET", url)
                    return True
                except Exception:
                    return False

    def get_dynamic_metadata(self, repo) -> Optional[Dict]:
        """
        Extract only dynamic metadata from a GitHub repository
        
        Args:
            repo: The GitHub repository object
            
        Returns:
            dict: Contains dynamic metadata fields
        """
        try:
            print(f"Extracting dynamic metadata for: {repo.full_name}")
            
            # Get releases
            releases = list(repo.get_releases())
            latest_release = releases[0] if releases else None
            
            # Get contributors count
            try:
                contributors = list(repo.get_contributors())
                contributors_count = len(contributors)
            except Exception as e:
                print(f"Error getting contributors count: {str(e)}")
                contributors_count = 0
            
            # Check if discussions are enabled
            discussions_enabled = self.check_discussions_enabled(repo)
            
            # Compile dynamic metadata
            metadata = {
                'url': repo.html_url,
                'repository_name': repo.full_name,
                'forks_count': repo.forks_count,
                'total_releases': len(releases),
                'latest_release': latest_release.tag_name if latest_release else 'No releases',
                'release_date': latest_release.created_at.strftime('%Y-%m-%d') if latest_release else 'No releases',
                'stars': repo.stargazers_count,
                'open_issues': repo.open_issues_count,
                'contributors_count': contributors_count,
                'discussions': discussions_enabled,
                'tstamp': datetime.utcnow().isoformat() + '+00:00'  # Timestamp of when this data was collected
            }
            
            return metadata
            
        except Exception as e:
            print(f"Error extracting metadata for {repo.full_name}: {str(e)}")
            return None

    def process_repositories_from_csv(self, csv_file_path: str, limit: int = REPO_LIMIT) -> List[Dict]:
        """
        Process repositories from CSV file and extract dynamic metadata
        
        Args:
            csv_file_path: Path to CSV file containing repository URLs
            limit: Maximum number of repositories to process
            
        Returns:
            List of dictionaries containing dynamic metadata
        """
        print(f"Loading repository URLs from {csv_file_path}...")
        
        # Load repository URLs from CSV
        try:
            df = pd.read_csv(csv_file_path)
            
            # Handle different possible column names
            url_column = None
            possible_columns = ['URL', 'url', 'URLs', 'repository_url', 'github_url']
            
            for col in possible_columns:
                if col in df.columns:
                    url_column = col
                    break
            
            if url_column is None:
                raise ValueError(f"CSV file must contain one of these columns: {possible_columns}. Found columns: {df.columns.tolist()}")
            
            # Extract repository URLs
            repo_urls = df[url_column].tolist()
            
            # Remove any blank or NaN values
            repo_urls = [url for url in repo_urls if url and isinstance(url, str) and url.strip()]
            
            print(f"Loaded {len(repo_urls)} repository URLs from CSV file")
            
        except Exception as e:
            print(f"Error loading CSV file: {str(e)}")
            return []
        
        # Apply limit
        if limit:
            repo_urls = repo_urls[:limit]
        
        total_count = len(repo_urls)
        results = []
        
        print(f"Processing {total_count} repositories...")
        
        for idx, repo_url in enumerate(repo_urls, 1):
            self.print_status(idx, total_count)
            
            try:
                # Extract full_name from URL (format: https://github.com/owner/repo)
                parsed_url = repo_url.strip().rstrip('/')
                full_name = '/'.join(parsed_url.split('/')[-2:])
                
                print(f"Processing ({idx}/{total_count}): {full_name}")
                
                # Get the repository object
                try:
                    repo = self.github.get_repo(full_name)
                except Exception as e:
                    print(f"Error getting repository {full_name}: {str(e)}")
                    self.error_count += 1
                    continue
                
                # Extract dynamic metadata
                metadata = self.get_dynamic_metadata(repo)
                
                if metadata:
                    results.append(metadata)
                    self.processed_count += 1
                else:
                    self.error_count += 1
                
                # Rate limiting protection
                if idx % 10 == 0:
                    rate_limit = self.github.get_rate_limit()
                    if rate_limit.core.remaining < 100:
                        reset_time = rate_limit.core.reset
                        sleep_time = (reset_time - datetime.now(reset_time.tzinfo)).total_seconds() + 60
                        print(f"Rate limit low ({rate_limit.core.remaining}), sleeping for {sleep_time/60:.1f} minutes")
                        time.sleep(sleep_time)
                    else:
                        time.sleep(1)  # Brief pause between requests
                
            except RateLimitExceededException:
                reset_time = self.github.get_rate_limit().core.reset
                sleep_time = (reset_time - datetime.now(reset_time.tzinfo)).total_seconds() + 60
                print(f"Rate limit exceeded. Sleeping for {sleep_time/60:.1f} minutes")
                time.sleep(sleep_time)
                continue
                
            except Exception as e:
                print(f"Error processing URL {repo_url}: {str(e)}")
                self.error_count += 1
                continue
        
        return results

    def save_results(self, results: List[Dict], output_file: str):
        """Save results to CSV file"""
        if not results:
            print("No data to save.")
            return
        
        try:
            df = pd.DataFrame(results)
            
            # Define column order for better readability
            column_order = [
                'repository_name',
                'url',
                'forks_count',
                'total_releases',
                'latest_release',
                'release_date',
                'stars',
                'open_issues',
                'contributors_count',
                'discussions',
                'tstamp'
            ]
            
            # Ensure all columns exist and reorder
            for col in column_order:
                if col not in df.columns:
                    df[col] = ''
            
            df = df[column_order]
            
            # Save to CSV
            df.to_csv(output_file, index=False)
            print(f"Results saved to {output_file}")
            print(f"Total repositories processed: {len(results)}")
            
        except Exception as e:
            print(f"Error saving results: {str(e)}")
            # Try alternative filename
            alt_filename = f"dynamic_metadata_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            try:
                df.to_csv(alt_filename, index=False)
                print(f"Results saved to alternative file: {alt_filename}")
            except Exception as e2:
                print(f"Failed to save to alternative file: {str(e2)}")


def main():
    """Main function to run the dynamic metadata updater"""
    
    # Configuration
    input_csv = "url_list.csv"  # Input CSV file with repository URLs
    output_csv = "dynamic_metadata_update.csv"  # Output CSV file
    
    print("CKAN Extensions Dynamic Metadata Updater")
    print("=" * 50)
    print(f"Input file: {input_csv}")
    print(f"Output file: {output_csv}")
    print(f"Repository limit: {REPO_LIMIT}")
    print()
    
    # Check if input file exists
    if not os.path.exists(input_csv):
        print(f"Error: Input file '{input_csv}' not found.")
        print("Please ensure the CSV file with repository URLs exists.")
        return
    
    # Validate GitHub token
    if not GITHUB_TOKEN or GITHUB_TOKEN == 'GH_METADATA_TOKEN':
        print("Error: Please set a valid GitHub token in the GITHUB_TOKEN variable.")
        return
    
    try:
        # Initialize updater
        updater = DynamicMetadataUpdater(GITHUB_TOKEN)
        
        # Process repositories
        print("Starting dynamic metadata extraction...")
        results = updater.process_repositories_from_csv(input_csv, REPO_LIMIT)
        
        # Save results
        if results:
            updater.save_results(results, output_csv)
            
            print("\n" + "=" * 50)
            print("SUMMARY")
            print("=" * 50)
            print(f"Total repositories processed: {updater.processed_count}")
            print(f"Total errors encountered: {updater.error_count}")
            print(f"Success rate: {(updater.processed_count / (updater.processed_count + updater.error_count) * 100):.1f}%")
            print(f"Results saved to: {output_csv}")
        else:
            print("No results to save. Please check the input file and try again.")
            
    except KeyboardInterrupt:
        print("\nProcess interrupted by user!")
        
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        traceback.print_exc()


if __name__ == "__main__":
    main()
