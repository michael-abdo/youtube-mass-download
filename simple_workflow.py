#!/usr/bin/env python3
"""
SIMPLE 6-STEP WORKFLOW - MINIMAL IMPLEMENTATION
Keep it simple. No over-engineering.
"""

import pandas as pd
import re
import argparse
import sys
import json
import pickle
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
import os
import urllib.parse
import time
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Import centralized configuration, path utilities, error handling, patterns, and CSV operations (DRY)
from utils.config import get_config, ensure_parent_dir, ensure_directory, format_error_message, load_json_state, save_json_state
from utils.patterns import PatternRegistry, extract_youtube_id, extract_drive_id, clean_url, normalize_whitespace, cleanup_selenium_driver, get_selenium_driver
from utils.extract_links import extract_google_doc_text, extract_actual_url, extract_text_with_retry
from utils.csv_manager import CSVManager
from utils.http_pool import get as http_get  # Centralized HTTP requests (DRY)
from utils.streaming_integration import stream_extracted_links
from utils.constants import CSVConstants, URLPatterns
from utils.s3_manager import UnifiedS3Manager, S3Config, UploadMode


# Configuration - centralized in config.yaml (DRY)
config = get_config()

# CSV operations now use centralized CSVManager (DRY)

# Selenium driver functions now imported from patterns.py (DRY consolidation)

# Progress tracking functions (DRY: using centralized state management)
# Progress management functions removed - now using centralized load_json_state/save_json_state (DRY)

def load_failed_docs():
    """Load failed document extraction list"""
    return load_json_state(
        config.get("paths.failed_extractions", "failed_extractions.json"),
        []
    )

def save_failed_docs(failed_list):
    """Save failed document extraction list"""
    save_json_state(
        config.get("paths.failed_extractions", "failed_extractions.json"),
        failed_list
    )

# Selenium driver functions moved to patterns.py (DRY consolidation)

def step1_download_sheet():
    """Step 1: Download a local copy of the Google Sheet"""
    print("Step 1: Downloading Google Sheet...")
    
    # First try HTTP request (faster)
    try:
        print("  Trying HTTP download...")
        response = http_get(config.get("google_sheets.url"))
        response.raise_for_status()
        html_content = response.text
        
        # Quick check if we got actual data
        soup = BeautifulSoup(html_content, "html.parser")
        
        # Look for the specific div with target ID
        target_div = soup.find("div", {"id": str(config.get("google_sheets.target_div_id"))})
        if target_div:
            table = target_div.find("table")
        else:
            # Fallback to any table
            table = soup.find("table", {"class": "waffle"})
            if not table:
                tables = soup.find_all("table")
                table = tables[0] if tables else None
        
        # Check if we found a table with rows
        if table:
            rows = table.find_all("tr")
            if len(rows) > 1:  # More than just header
                # Save the HTML
                sheet_cache_path = get_config().get('paths.sheet_cache', 'sheet.html')
                with open(sheet_cache_path, "w", encoding="utf-8") as f:
                    f.write(html_content)
                
                print(f"  ✓ Sheet downloaded via HTTP (found {len(rows)} rows)")
                return html_content
        
        print("  ✗ HTTP download incomplete (no table data found)")
        print("  Falling back to Selenium...")
        
    except Exception as e:
        print(f"  ✗ HTTP download failed: {e}")
        print("  Falling back to Selenium...")
    
    # Fallback to Selenium for JavaScript-rendered content
    driver = None
    try:
        driver = get_selenium_driver()
        
        # Navigate directly to the PROFILE LIST sheet tab
        sheet_url = "https://docs.google.com/spreadsheets/u/1/d/e/2PACX-1vRqqjqoaj8sEZBfZRw0Og7g8ms_0yTL2MsegTubcjhhBnXr1s1jFBwIVAsbkyj1xD0TMj06LvGTQIHU/pubhtml/sheet?pli=1&headers=false&gid=1159146182"
        
        driver.get(sheet_url)
        
        # Wait for the table to load
        wait = WebDriverWait(driver, 20)
        
        # Try to find the table with the specific div ID first
        target_div_id = str(config.get("google_sheets.target_div_id"))
        try:
            target_div = wait.until(
                EC.presence_of_element_located((By.ID, target_div_id))
            )
            print(f"  ✓ Found target div with ID: {target_div_id}")
        except:
            print(f"  ✗ Target div {target_div_id} not found, looking for any table...")
            
        # Wait for any table to be present
        wait.until(
            EC.presence_of_element_located((By.TAG_NAME, "table"))
        )
        
        # Additional wait to ensure data is loaded
        time.sleep(3)
        
        # Try to find table with waffle class (Google Sheets specific)
        try:
            wait.until(
                EC.presence_of_element_located((By.CLASS_NAME, "waffle"))
            )
            print("  ✓ Found Google Sheets table")
        except:
            print("  ✗ No waffle table found, using any table")
        
        # Get the page source after JavaScript has executed
        html_content = driver.page_source
        
        # Save the HTML
        sheet_cache_path = get_config().get('paths.sheet_cache', 'sheet.html')
        with open(sheet_cache_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        
        print("✓ Sheet downloaded with Selenium")
        return html_content
        
    except Exception as e:
        print(f"✗ Failed to download sheet with Selenium: {e}")
        raise Exception("Failed to download sheet with both HTTP and Selenium methods")
        
    finally:
        if driver:
            driver.quit()

def step2_extract_people_and_docs(html_content):
    """Step 2: Extract people data and Google Doc links from the sheet"""
    print("Step 2: Extracting people data and Google Doc links...")
    
    soup = BeautifulSoup(html_content, "html.parser")
    
    # Look for the specific div with target ID
    target_div = soup.find("div", {"id": str(config.get("google_sheets.target_div_id"))})
    if target_div:
        table = target_div.find("table")
    else:
        # Fallback to any table
        table = soup.find("table", {"class": "waffle"})
        if not table:
            tables = soup.find_all("table")
            table = tables[0] if tables else None
    
    people_data = []
    if table:
        rows = table.find_all("tr")
        print(f"Found {len(rows)} rows in the table")
        
        # Process rows starting from row 1 (skip header)
        for row_index in range(1, len(rows)):
            row = rows[row_index]
            cells = row.find_all("td")
            
            # Need at least 5 cells (row_id, name, email, type)
            if len(cells) < 5:
                continue
            
            # Extract data using the correct column indices from the working code
            row_id = cells[0].get_text(strip=True)
            name = cells[2].get_text(strip=True)  # Name in column 2
            email = cells[3].get_text(strip=True)  # Email in column 3  
            type_val = cells[4].get_text(strip=True)  # Type in column 4
            
            # Skip header rows and invalid data
            if not name or name.lower() == "name" or row_id == "#" or "name" in name.lower() and "email" in email.lower():
                continue
            
            # Skip any row that looks like a header (contains "Name", "Email", "Type" pattern)
            if any(["Name" in str(cell.get_text(strip=True)) and "Email" in str(cells[3].get_text(strip=True)) and "Type" in str(cells[4].get_text(strip=True)) for cell in cells]):
                continue
            
            # Look for Google Doc link in the name cell
            doc_link = None
            name_cell = cells[2]
            a_tags = name_cell.find_all("a")
            if a_tags:
                a_tag = a_tags[0]
                if a_tag.has_attr("href"):
                    href = a_tag["href"]
                    if href.startswith("https://www.google.com/url?q="):
                        doc_link = extract_actual_url(href)
            
            people_data.append({
                "row_id": row_id,
                "name": name,
                "email": email,
                "type": type_val,
                "doc_link": doc_link if doc_link else ""
            })
    
    print(f"✓ Found {len(people_data)} people records")
    
    # Filter to only those with actual Google Doc links (not direct YouTube/Drive links)
    people_with_docs = []
    people_with_direct_links = []
    
    for person in people_data:
        if person["doc_link"]:
            # Check if it's a Google Doc or a direct YouTube/Drive link
            link = person["doc_link"].lower()
            if "docs.google.com/document" in link:
                people_with_docs.append(person)
            elif "youtube.com" in link or "youtu.be" in link or "drive.google.com/file" in link:
                people_with_direct_links.append(person)
            else:
                # Unknown link type, treat as doc for safety
                people_with_docs.append(person)
    
    print(f"✓ Found {len(people_with_docs)} people with Google Doc links")
    print(f"✓ Found {len(people_with_direct_links)} people with direct YouTube/Drive links")
    
    return people_data, people_with_docs

def step3_scrape_doc_contents(doc_url):
    """Step 3: Scrape contents and text of a Google Doc"""
    print(f"Step 3: Scraping doc: {doc_url}")
    
    # For Google Docs, use Selenium to get both HTML and text
    if "docs.google.com/document" in doc_url:
        # Extract the document text using Selenium
        doc_text = extract_google_doc_text(doc_url)
        
        # Also get the HTML for link extraction
        try:
            response = http_get(doc_url)  # Use centralized HTTP pool (DRY)
            response.raise_for_status()
            html_content = response.text
            print("✓ Doc scraped successfully (HTML + text)")
            return html_content, doc_text
        except Exception as e:
            print(f"✗ Failed to scrape HTML: {e}")
            return "", doc_text
    else:
        # For other URLs, just get HTML
        try:
            response = http_get(doc_url)  # Use centralized HTTP pool (DRY)
            response.raise_for_status()
            print("✓ Doc scraped successfully (HTML only)")
            return response.text, ""
        except Exception as e:
            print(f"✗ Failed to scrape doc: {e}")
            return "", ""

def step4_extract_links(doc_content, doc_text=""):
    """Step 4: Extract links from scraped content and document text"""
    print("Step 4: Extracting links from doc content...")
    
    # Combine HTML content and plain text for comprehensive link extraction
    combined_content = doc_content + " " + doc_text
    
    # Decode Unicode escapes commonly found in Google Docs HTML
    try:
        combined_content = combined_content.encode('utf-8').decode('unicode-escape')
    except:
        # If decoding fails, continue with original content
        pass
    
    links = {
        'youtube': [],
        'drive_files': [],
        'drive_folders': [],
        'all_links': []
    }
    
    # Use centralized YouTube patterns (DRY)
    youtube_patterns = [
        PatternRegistry.YOUTUBE_VIDEO_FULL,
        PatternRegistry.YOUTUBE_SHORT_FULL,
        PatternRegistry.YOUTUBE_PLAYLIST_FULL
    ]
    
    for pattern in youtube_patterns:
        matches = pattern.findall(combined_content)
        for match in matches:
            if pattern == PatternRegistry.YOUTUBE_PLAYLIST_FULL:
                clean_link = URLPatterns.youtube_playlist_url(match)
            else:
                clean_link = URLPatterns.youtube_watch_url(match)
            
            if clean_link not in links['youtube']:
                links['youtube'].append(clean_link)
    
    # Also try to find YouTube playlists with Unicode escapes (common in Google Docs)
    import re
    escaped_playlist_pattern = re.compile(r'youtube\.com/playlist\?list\\u003d([a-zA-Z0-9_-]+)')
    escaped_matches = escaped_playlist_pattern.findall(combined_content)
    for match in escaped_matches:
        clean_link = URLPatterns.youtube_playlist_url(match)
        if clean_link not in links['youtube']:
            links['youtube'].append(clean_link)
    
    # Use centralized Google Drive patterns (DRY)
    drive_patterns = [
        PatternRegistry.DRIVE_FILE_FULL,
        PatternRegistry.DRIVE_OPEN_FULL,
        PatternRegistry.DRIVE_FOLDER_FULL
    ]
    
    for pattern in drive_patterns:
        matches = pattern.findall(combined_content)
        for match in matches:
            if pattern == PatternRegistry.DRIVE_FOLDER_FULL:
                clean_link = URLPatterns.drive_folder_url(match)
                if clean_link not in links['drive_folders']:
                    links['drive_folders'].append(clean_link)
            else:
                clean_link = URLPatterns.drive_file_url(match, view=True)
                if clean_link not in links['drive_files']:
                    links['drive_files'].append(clean_link)
    
    # Extract all HTTP(S) links for comprehensive coverage using centralized pattern (DRY)
    all_found_links = PatternRegistry.HTTP_URL.findall(combined_content)
    
    # Clean and categorize all links
    for link in all_found_links:
        clean_link = clean_url(link)
        if clean_link and clean_link not in links['all_links']:
            links['all_links'].append(clean_link)
            
            # Additional categorization for missed links
            if ('youtube.com' in clean_link or 'youtu.be' in clean_link) and clean_link not in links['youtube']:
                links['youtube'].append(clean_link)
            elif 'drive.google.com/file' in clean_link and clean_link not in links['drive_files']:
                links['drive_files'].append(clean_link)
            elif 'drive.google.com/drive/folders' in clean_link and clean_link not in links['drive_folders']:
                links['drive_folders'].append(clean_link)
    
    # Remove duplicates
    links['youtube'] = list(set(links['youtube']))
    links['drive_files'] = list(set(links['drive_files']))
    links['drive_folders'] = list(set(links['drive_folders']))
    links['all_links'] = list(set(links['all_links']))
    
    total_links = len(links['youtube']) + len(links['drive_files']) + len(links['drive_folders'])
    print(f"✓ Found {total_links} targeted links (YT: {len(links['youtube'])}, Files: {len(links['drive_files'])}, Folders: {len(links['drive_folders'])})")
    print(f"✓ Found {len(links['all_links'])} total links")
    
    return links

def filter_meaningful_links(links):
    """Filter extracted links to only meaningful content links (like operators do)"""
    print("  Filtering meaningful links...")
    
    # Define infrastructure/noise patterns to exclude
    noise_patterns = [
        # Google infrastructure
        r'accounts\.google\.com',
        r'apis\.google\.com', 
        r'clients6\.google\.com',
        r'gstatic\.com',
        r'googleapis\.com',
        r'googleusercontent\.com',
        r'ogs\.google\.com',
        r'ogads-pa\.clients6\.google\.com',
        r'people-pa\.clients6\.google\.com',
        r'addons.*\.google\.com',
        r'workspace\.google\.com',
        r'myaccount\.google\.com',
        r'contacts\.google\.com',
        r'script\.google\.com',
        r'drivefrontend-pa\.clients6\.google\.com',
        
        # Chrome extensions and static resources
        r'chrome\.google\.com/webstore',
        r'docs\.google\.com/static/',
        r'docs\.google\.com/persistent/',
        r'docs\.google\.com/relay\.html',
        r'docs\.google\.com/picker',
        r'docs\.google\.com/drawings',
        
        # Document-specific URLs (not content)
        r'docs\.google\.com/document/.*edit',
        r'docs\.google\.com/document/.*preview',
        r'docs\.google\.com/document/\?usp=docs_web',
        r'&amp;usp=embed_',
        r'\?tab=t\.',
        
        # Schema.org and other metadata
        r'schema\.org',
        r'meet\.google\.com',
        
        # Non-content file extensions
        r'\.js$',
        r'\.css$',
        r'\.woff2?$',
        r'\.ico$',
        r'\.gif$',
        r'\.binarypb$',
        r'\.model$'
    ]
    
    def is_meaningful_link(link):
        """Check if a link is meaningful content vs infrastructure noise"""
        link_lower = link.lower()
        
        # Check against noise patterns
        for pattern in noise_patterns:
            if re.search(pattern, link):
                return False
        
        # Keep YouTube content links
        if any(domain in link_lower for domain in ['youtube.com', 'youtu.be']):
            # Must be actual video or playlist, not just any YouTube URL
            return bool(re.search(r'(watch\?v=|playlist\?list=|youtu\.be/[a-zA-Z0-9_-]{11})', link))
        
        # Keep Drive files and folders (but not just drive.google.com root)
        if 'drive.google.com' in link_lower:
            return bool(re.search(r'(file/d/[a-zA-Z0-9_-]+|drive/folders/[a-zA-Z0-9_-]+)', link))
        
        return False
    
    # Filter all link categories
    meaningful_youtube = []
    meaningful_drive_files = []
    meaningful_drive_folders = []
    
    # Process YouTube links
    for link in links.get('youtube', []):
        if is_meaningful_link(link):
            # Normalize YouTube URLs to standard format using centralized extraction
            if '/watch?v=' in link:
                video_id = extract_youtube_id(link)
                if video_id:
                    meaningful_youtube.append(URLPatterns.youtube_watch_url(video_id))
            elif '/playlist?list=' in link:
                match = re.search(r'list=([a-zA-Z0-9_-]+)', link)
                if match:
                    meaningful_youtube.append(URLPatterns.youtube_playlist_url(match.group(1)))
            elif 'youtu.be/' in link:
                video_id = extract_youtube_id(link)
                if video_id:
                    meaningful_youtube.append(URLPatterns.youtube_watch_url(video_id))
    
    # Process Drive files
    for link in links.get('drive_files', []):
        if is_meaningful_link(link):
            # Normalize Drive file URLs using centralized extraction
            file_id = extract_drive_id(link)
            if file_id:
                meaningful_drive_files.append(URLPatterns.drive_file_url(file_id, view=True))
    
    # Process Drive folders  
    for link in links.get('drive_folders', []):
        if is_meaningful_link(link):
            # Normalize Drive folder URLs using centralized extraction
            folder_id = extract_drive_id(link)
            if folder_id:
                meaningful_drive_folders.append(URLPatterns.drive_folder_url(folder_id))
    
    # Also check all_links for any missed content links
    for link in links.get('all_links', []):
        if is_meaningful_link(link):
            if any(pattern in link.lower() for pattern in ['youtube.com', 'youtu.be']) and link not in meaningful_youtube:
                # Process as YouTube using centralized extraction
                video_id = extract_youtube_id(link)
                if video_id:
                    meaningful_youtube.append(URLPatterns.youtube_watch_url(video_id))
                elif '/playlist?list=' in link:
                    match = re.search(r'list=([a-zA-Z0-9_-]+)', link)
                    if match:
                        meaningful_youtube.append(URLPatterns.youtube_playlist_url(match.group(1)))
            elif 'drive.google.com/file' in link and link not in meaningful_drive_files:
                file_id = extract_drive_id(link)
                if file_id:
                    meaningful_drive_files.append(URLPatterns.drive_file_url(file_id, view=True))
            elif 'drive.google.com/drive/folders' in link and link not in meaningful_drive_folders:
                folder_id = extract_drive_id(link)
                if folder_id:
                    meaningful_drive_folders.append(URLPatterns.drive_folder_url(folder_id))
    
    # Remove duplicates and sort
    meaningful_youtube = sorted(list(set(meaningful_youtube)))
    meaningful_drive_files = sorted(list(set(meaningful_drive_files))) 
    meaningful_drive_folders = sorted(list(set(meaningful_drive_folders)))
    
    print(f"    Filtered: {len(meaningful_youtube)} YouTube, {len(meaningful_drive_files)} Drive files, {len(meaningful_drive_folders)} Drive folders")
    
    return {
        'youtube': meaningful_youtube,
        'drive_files': meaningful_drive_files,
        'drive_folders': meaningful_drive_folders
    }

def step5_process_extracted_data(person, links, doc_text=""):
    """Step 5: Process extracted data and stream to S3, then format for CSV"""
    print("Step 5: Processing extracted data...")
    
    # Use centralized path utility (DRY)
    ensure_directory(config.get("paths.output_dir", "simple_downloads"))
    
    # Filter links to get only meaningful content links (like operators do)
    meaningful_links = filter_meaningful_links(links)
    
    # Check if we have any links to stream
    total_links = (len(meaningful_links['youtube']) + 
                  len(meaningful_links['drive_files']) + 
                  len(meaningful_links['drive_folders']))
    
    s3_uuids = None
    
    if total_links > 0 and config.get("downloads.storage_mode") == "s3":
        print(f"  🚀 Streaming {total_links} links directly to S3...")
        
        # Initialize S3 manager
        s3_config = S3Config(
            bucket_name=config.get("downloads.s3.default_bucket", "typing-clients-uuid-system"),
            upload_mode=UploadMode.DIRECT_STREAMING,
            organize_by_person=False,
            add_metadata=True
        )
        s3_manager = UnifiedS3Manager(s3_config)
        
        # Stream links to S3 and get UUID mappings
        try:
            s3_uuids = stream_extracted_links(person, meaningful_links, s3_manager)
            print(f"  ✅ Successfully streamed {len(s3_uuids.get('file_uuids', {}))} files to S3")
        except Exception as e:
            print(f"  ❌ Error streaming to S3: {str(e)}")
            print(f"  ⚠️ Falling back to URL storage in CSV")
    
    # Prepare links data for factory (for backward compatibility)
    links_data = {
        'youtube': meaningful_links['youtube'],
        'drive_files': meaningful_links['drive_files'],
        'drive_folders': meaningful_links['drive_folders'],
        'all_links': links['all_links']
    }
    
    # Create record with S3 UUIDs if available, otherwise with URLs
    record = CSVManager.create_record(
        person, 
        mode='full', 
        doc_text=doc_text, 
        links=links_data,
        s3_uuids=s3_uuids  # Pass S3 UUIDs if streaming was successful
    )
    
    print(f"✓ Processed record for {person['name']}")
    print(f"  Meaningful YouTube links: {len(meaningful_links['youtube'])}")
    print(f"  Drive Files: {len(meaningful_links['drive_files'])}")
    print(f"  Drive Folders: {len(meaningful_links['drive_folders'])}")
    if s3_uuids:
        print(f"  S3 files uploaded: {len(s3_uuids.get('file_uuids', {}))}")
    print(f"  Total links extracted: {len(links['all_links'])}")
    
    return record


# extract_text_with_retry function moved to utils/extract_links.py (DRY consolidation)

def step6_map_data(processed_records, basic_mode=False, text_mode=False, output_file=None):
    """Step 6: Map data to CSV"""
    print("Step 6: Mapping data to CSV...")
    
    # Handle different column sets based on processing mode (DRY: use config)
    if basic_mode:
        # Basic mode: only 5 columns
        required_columns = config.get('csv_columns.basic')
    elif text_mode:
        # Text mode: basic columns + document text + processing info
        required_columns = config.get('csv_columns.text')
    else:
        # Full mode: all columns matching main system
        required_columns = config.get('csv_columns.full')
    
    # Ensure all required columns are present in records
    for record in processed_records:
        for col in required_columns:
            if col not in record:
                record[col] = ''
    
    # Filter records to only include required columns in correct order
    filtered_records = []
    for record in processed_records:
        filtered_record = {col: record.get(col, '') for col in required_columns}
        filtered_records.append(filtered_record)
    
    # Determine output file
    if not output_file:
        if basic_mode:
            output_file = config.get("paths.output_csv", "simple_output.csv")
        elif text_mode:
            output_file = "text_extraction_output.csv"
        else:
            output_file = config.get("paths.output_csv", "simple_output.csv")
    
    # Create DataFrame for CSV operations
    df = pd.DataFrame(filtered_records)
    
    # Write to CSV
    print("  📄 Writing to CSV...")
    csv_manager = CSVManager(csv_path=output_file)
    csv_success = csv_manager.safe_csv_write(df, operation_name="step6_workflow_output")
    
    if csv_success:
        print(f"  ✅ Data saved to {output_file}")
    else:
        print(f"  ❌ Failed to save data to {output_file}")
        return None
    
    print(f"  📊 Total records: {len(df)}")
    print(f"  📊 Records with links: {len(df[df[CSVConstants.Columns.LINK] != ''])}")
    
    # Additional stats only for full mode
    if not basic_mode and not text_mode:
        print(f"  📊 Records with YouTube: {len(df[df[CSVConstants.Columns.YOUTUBE_PLAYLIST] != ''])}")
        print(f"  📊 Records with Drive: {len(df[df[CSVConstants.Columns.GOOGLE_DRIVE] != ''])}")
    
    # Text mode specific stats
    if text_mode:
        if 'document_text' in df.columns:
            successful_extractions = len(df[(df[CSVConstants.Columns.DOCUMENT_TEXT] != '') & (~df[CSVConstants.Columns.DOCUMENT_TEXT].str.startswith('EXTRACTION_FAILED', na=False))])
            failed_extractions = len(df[df[CSVConstants.Columns.DOCUMENT_TEXT].str.startswith('EXTRACTION_FAILED', na=False)])
            print(f"  📊 Successful text extractions: {successful_extractions}")
            print(f"  📊 Failed text extractions: {failed_extractions}")
    
    return df


def update_csv_incrementally(all_records, current_index, record, basic_mode=False, text_mode=False, output_file=None):
    """Update CSV incrementally after each successful S3 process"""
    # Update the record at the current index
    all_records[current_index] = record
    
    # Handle different column sets based on processing mode
    if basic_mode:
        required_columns = config.get('csv_columns.basic')
    elif text_mode:
        required_columns = config.get('csv_columns.text')
    else:
        required_columns = config.get('csv_columns.full')
    
    # Ensure all required columns are present in all records
    for rec in all_records:
        for col in required_columns:
            if col not in rec:
                rec[col] = ''
    
    # Filter records to only include required columns in correct order
    filtered_records = []
    for rec in all_records:
        filtered_record = {col: rec.get(col, '') for col in required_columns}
        filtered_records.append(filtered_record)
    
    # Determine output file
    if not output_file:
        if basic_mode:
            output_file = config.get("paths.output_csv", "simple_output.csv")
        elif text_mode:
            output_file = "text_extraction_output.csv"
        else:
            output_file = config.get("paths.output_csv", "simple_output.csv")
    
    # Create DataFrame for CSV operations
    df = pd.DataFrame(filtered_records)
    
    # Write to CSV
    csv_manager = CSVManager(csv_path=output_file)
    csv_success = csv_manager.safe_csv_write(df, operation_name=f"incremental_update_{current_index}")
    
    if not csv_success:
        print(f"  ❌ Failed to update CSV after processing record {current_index}")
    
    return csv_success

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Simple 6-Step Workflow - Unified Processing')
    
    # Processing mode options (mutually exclusive)
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--basic', action='store_true', 
                           help='Extract only basic columns (row_id, name, email, type, link)')
    mode_group.add_argument('--text', action='store_true',
                           help='Extract basic columns + document text (batch processing)')
    
    # Processing options
    parser.add_argument('--test-limit', type=int, metavar='N',
                       help='Limit processing to N records for testing')
    parser.add_argument('--batch-size', type=int, metavar='N', default=10,
                       help='Process N documents per batch (default: 10)')
    parser.add_argument('--resume', action='store_true',
                       help='Resume from previous extraction progress')
    parser.add_argument('--retry-failed', action='store_true',
                       help='Retry previously failed extractions')
    parser.add_argument('--output', type=str, metavar='FILE',
                       help='Override output CSV filename')
    parser.add_argument('--no-yt-dlp-update', action='store_true',
                       help='Skip automatic yt-dlp update before processing')
    
    return parser.parse_args()

def main():
    """Run the complete 6-step workflow"""
    # Parse command line arguments
    args = parse_arguments()
    
    # Configure based on arguments
    basic_mode = args.basic
    text_mode = args.text
    batch_size = args.batch_size
    test_limit = args.test_limit if args.test_limit else None
    output_file = args.output if args.output else config.get("paths.output_csv", "simple_output.csv")
    
    # Display configuration
    if basic_mode:
        mode = "BASIC MODE"
    elif text_mode:
        mode = f"TEXT EXTRACTION MODE (batch size: {batch_size})"
    else:
        mode = "FULL MODE"
    
    limit_text = f" (limited to {test_limit})" if test_limit else ""
    resume_text = " [RESUMING]" if args.resume else ""
    retry_text = " [RETRY FAILED]" if args.retry_failed else ""
    
    print(f"STARTING SIMPLE 6-STEP WORKFLOW - {mode}{limit_text}{resume_text}{retry_text}")
    print("=" * 80)
    
    processed_records = []
    
    # Step 1: Download sheet
    html_content = step1_download_sheet()
    
    # Step 2: Extract people data and Google Doc links
    all_people, people_with_docs = step2_extract_people_and_docs(html_content)
    
    print(f"\nProcessing ALL {len(all_people)} people...")
    print(f"  - {len(people_with_docs)} people have documents")
    print(f"  - {len(all_people) - len(people_with_docs)} people without documents")
    
    # Create a lookup for people with docs for efficient processing
    people_with_docs_dict = {person['row_id']: person for person in people_with_docs}
    
    # Pre-initialize all records with basic data for incremental updates
    all_records = []
    for person in all_people:
        # Create basic record for each person
        basic_record = CSVManager.create_record(person, mode='basic')
        all_records.append(basic_record)
    
    # Determine processing approach based on mode
    if basic_mode:
        print(f"\n🚀 BASIC MODE: Processing {len(all_people)} people (basic data only)...")
        # Basic processing - just write the pre-initialized records
        people_to_process = all_people[:test_limit] if test_limit else all_people
        
        # Write all basic records in one go for basic mode
        if all_records:
            print("  📝 Writing all basic records to CSV...")
            step6_map_data(all_records[:len(people_to_process)], basic_mode=basic_mode, text_mode=text_mode, output_file=output_file)
        
        print(f"✓ Processed {len(people_to_process)} records in basic mode")
    
    elif text_mode:
        print(f"\n🚀 TEXT EXTRACTION MODE: Processing {len(people_with_docs)} documents...")
        
        # Load previous progress if resuming
        # Load progress using centralized state management (DRY)
        default_progress = {"completed": [], "failed": [], "last_batch": 0, "total_processed": 0}
        progress = load_json_state(config.get("paths.extraction_progress", "extraction_progress.json"), default_progress) if args.resume else default_progress
        failed_docs = load_failed_docs() if args.retry_failed else []
        
        # Update all_records to text mode format
        for i, person in enumerate(all_people):
            # Update basic record to text mode record
            text_record = CSVManager.create_record(person, mode='text')
            all_records[i] = text_record
        
        # Write initial CSV with all records in text mode format
        print("\n📝 Writing initial CSV with text mode columns...")
        update_csv_incrementally(all_records, 0, all_records[0], basic_mode=basic_mode, text_mode=text_mode, output_file=output_file)
        
        # Determine which documents to process
        if args.retry_failed and failed_docs:
            docs_to_process = [person for person in people_with_docs if person['doc_link'] in failed_docs]
            print(f"  Retrying {len(docs_to_process)} previously failed documents...")
        elif args.resume:
            docs_to_process = [person for person in people_with_docs if person['doc_link'] not in progress['completed']]
            print(f"  Resuming: {len(docs_to_process)} remaining documents...")
        else:
            docs_to_process = people_with_docs
            print(f"  Processing all {len(docs_to_process)} documents...")
        
        # Apply test limit if specified
        if test_limit:
            docs_to_process = docs_to_process[:test_limit]
            print(f"  Limited to {len(docs_to_process)} documents for testing")
        
        # Process documents in batches
        current_failed = []
        batch_start = progress.get('last_batch', 0) if args.resume else 0
        
        for i in range(batch_start, len(docs_to_process), batch_size):
            batch = docs_to_process[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(docs_to_process) + batch_size - 1) // batch_size
            
            print(f"\n📦 BATCH {batch_num}/{total_batches} ({len(batch)} documents)")
            print("-" * 50)
            
            for j, person in enumerate(batch):
                doc_index = i + j + 1
                print(f"\n[{doc_index}/{len(docs_to_process)}] Processing: {person['name']}")
                print(f"  Document: {person['doc_link']}")
                
                # Extract text with retry logic
                doc_text, error = extract_text_with_retry(person['doc_link'])
                
                if error:
                    print(f"  ✗ Failed: {error}")
                    current_failed.append(person['doc_link'])
                    progress['failed'].append(person['doc_link'])
                    record = CSVManager.create_error_record(person, mode='text', error_message=error)
                else:
                    print(f"  ✓ Success: {len(doc_text)} characters extracted")
                    progress['completed'].append(person['doc_link'])
                    record = CSVManager.create_record(person, mode='text', doc_text=doc_text)
                
                # Find the index in all_records for this person
                record_index = next((idx for idx, rec in enumerate(all_records) if rec['row_id'] == person['row_id']), -1)
                if record_index >= 0:
                    # Update CSV incrementally after each document extraction
                    print("  📝 Updating CSV...")
                    update_csv_incrementally(all_records, record_index, record, basic_mode=basic_mode, text_mode=text_mode, output_file=output_file)
                
                progress['total_processed'] += 1
                
                # Add delay between documents
                if j < len(batch) - 1:  # Don't delay after last document in batch
                    delay = config.get("retry.base_delay", 2.0)
                    time.sleep(delay)
            
            # Save progress after each batch
            progress['last_batch'] = i + batch_size
            # Save progress using centralized state management (DRY)
            save_json_state(config.get("paths.extraction_progress", "extraction_progress.json"), progress)
            save_failed_docs(current_failed)
            
            print(f"\n✓ Batch {batch_num} complete")
            batch_records = processed_records[-len(batch):]
            successful_in_batch = len([r for r in batch_records if not r.get('document_text', '').startswith('EXTRACTION_FAILED')])
            failed_in_batch = len(batch) - successful_in_batch
            print(f"  Successful: {successful_in_batch}")
            print(f"  Failed: {failed_in_batch}")
        
        print(f"\n🎉 TEXT EXTRACTION COMPLETE")
        print(f"  Total processed: {progress['total_processed']}")
        print(f"  Successful extractions: {len(progress['completed'])}")
        print(f"  Failed extractions: {len(current_failed)}")
    
    else:
        print(f"\n🚀 FULL MODE: Processing {len(all_people)} people (with document processing)...")
        # Full processing of all people (both with and without docs)
        people_to_process = all_people[:test_limit] if test_limit else all_people
        
        # Write initial CSV with all basic records
        print("\n📝 Writing initial CSV with basic data for all people...")
        update_csv_incrementally(all_records, 0, all_records[0], basic_mode=basic_mode, text_mode=text_mode, output_file=output_file)
        
        for i, person in enumerate(people_to_process):
            print(f"\nProcessing person {i+1}/{len(people_to_process)}: {person['name']} (Row {person.get('row_id', 'Unknown')})")
            
            # Find the index in all_records for this person
            record_index = next((idx for idx, rec in enumerate(all_records) if rec['row_id'] == person['row_id']), i)
            
            # Check if this person has a link
            if person.get('doc_link'):
                link = person['doc_link'].lower()
                
                # Check if it's a Google Doc that needs scraping
                if person.get('row_id') in people_with_docs_dict:
                    print(f"  → Has Google Doc: {person['doc_link']}")
                    
                    # Step 3: Scrape doc content and text
                    doc_content, doc_text = step3_scrape_doc_contents(person['doc_link'])
                    
                    # Step 4: Extract links from HTML content and document text
                    links = step4_extract_links(doc_content, doc_text)
                    
                    # Step 5: Process extracted data (includes S3 streaming)
                    record = step5_process_extracted_data(person, links, doc_text)
                    
                    # Update CSV incrementally after successful S3 process
                    print("  📝 Updating CSV...")
                    update_csv_incrementally(all_records, record_index, record, basic_mode=basic_mode, text_mode=text_mode, output_file=output_file)
                
                # Handle direct YouTube/Drive links (Case 2)
                elif "youtube.com" in link or "youtu.be" in link or "drive.google.com/file" in link:
                    print(f"  → Has direct link: {person['doc_link']}")
                    
                    # For direct links, create the links structure directly without scraping
                    links = {
                        'youtube': [],
                        'drive_files': [],
                        'drive_folders': [],
                        'all_links': []
                    }
                    
                    # Add the direct link to appropriate category
                    if "youtube.com" in link or "youtu.be" in link:
                        links['youtube'].append(person['doc_link'])
                    elif "drive.google.com/file" in link:
                        links['drive_files'].append(person['doc_link'])
                    
                    links['all_links'].append(person['doc_link'])
                    
                    # Process without doc scraping (includes S3 streaming)
                    record = step5_process_extracted_data(person, links, '')
                    
                    # Update CSV incrementally after successful S3 process
                    print("  📝 Updating CSV...")
                    update_csv_incrementally(all_records, record_index, record, basic_mode=basic_mode, text_mode=text_mode, output_file=output_file)
                
                else:
                    print(f"  → Has unknown link type: {person['doc_link']}")
                    # Unknown link type, process as doc for safety
                    doc_content, doc_text = step3_scrape_doc_contents(person['doc_link'])
                    links = step4_extract_links(doc_content, doc_text)
                    record = step5_process_extracted_data(person, links, doc_text)
                    
                    # Update CSV incrementally after successful S3 process
                    print("  📝 Updating CSV...")
                    update_csv_incrementally(all_records, record_index, record, basic_mode=basic_mode, text_mode=text_mode, output_file=output_file)
            else:
                print(f"  → No document")
                # Create record for person without document using factory (DRY)
                record = CSVManager.create_record(person, mode='full', doc_text='', links=None)
                
                # Update CSV incrementally (even for no-doc cases to maintain consistency)
                update_csv_incrementally(all_records, record_index, record, basic_mode=basic_mode, text_mode=text_mode, output_file=output_file)
    
    # Step 6 is now done incrementally, so just print summary
    print("\n" + "=" * 50)
    print("📊 FINAL SUMMARY")
    print(f"  Total people processed: {len(people_to_process) if 'people_to_process' in locals() else len(processed_records)}")
    print(f"  CSV updated incrementally after each S3 process")
    print(f"  Final CSV location: {output_file}")
    
    # Cleanup Selenium driver
    cleanup_selenium_driver()
    
    print("\n" + "=" * 50)
    print("WORKFLOW COMPLETE")

if __name__ == "__main__":
    main()