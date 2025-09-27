#!/usr/bin/env python3
"""
TEST ALL 30 ROWS
Comprehensive test of extraction against truth source for rows 501-472
"""

from utils.path_setup import init_project_imports
init_project_imports()

from simple_workflow import step3_scrape_doc_contents, step4_extract_links, cleanup_selenium_driver
from utils.patterns import normalize_url_for_truth_comparison, compare_urls_for_truth
import json
import time
import re

# Complete truth source data from user
TRUTH_SOURCE = {
    501: {"name": "Dmitriy Golovko", "type": "No asset", "links": []},
    500: {"name": "Seth Dossett", "type": "No asset", "links": []},
    499: {"name": "Carlos Arthur", "type": "YouTube video", "links": ["https://www.youtube.com/watch?v=UD2X2hJTq4Y"]},
    498: {"name": "Caroline Chiu", "type": "Google Doc → Google Drive video file", 
          "links": ["https://drive.google.com/file/d/1c13knvRxfF-HrQhyBOfGIZpcrY-OR3_A/view"],
          "doc_url": "https://docs.google.com/document/d/1Ael_iSce9tO3SECHp5X5N0yYgahDnnCY837aYsC21PE/edit?tab=t.0"},
    497: {"name": "James Kirton", "type": "Google Doc → 2 YouTube playlists",
          "links": [
              "https://youtube.com/playlist?list=PLp0u93QMy5vCYzE4OQydGEMEJjRizGDA",
              "https://youtube.com/playlist?list=PLu9i8x5U9PHhmD9K-5WY4EB12vyhL"
          ],
          "doc_url": "https://docs.google.com/document/d/106UzVKTBceNnihO711snD-AeJOK9-fki9EINggkLQU8/edit?tab=t.0"},
    496: {"name": "Florence", "type": "Google Doc (no video links)", "links": []},
    495: {"name": "John Williams", "type": "Google Doc → 4 YouTube videos",
          "links": [
              "https://www.youtube.com/watch?v=K6kBTbjH4cI",
              "https://youtu.be/vHD2wDyrWLw",
              "https://youtu.be/BlSxvQ9p8Q0",
              "https://youtu.be/ZBuf3DGBuM"
          ]},
    494: {"name": "Maddie Boyle", "type": "Google Doc → YouTube video",
          "links": ["https://www.youtube.com/watch?v=4HfkyhvaaWc"]},
    493: {"name": "Kiko", "type": "Google Drive folder (video file inside)",
          "links": ["https://drive.google.com/drive/folders/1-4mmoEIuZKq4xOuJpzeBdnxbkYZQJZTl"]},
    492: {"name": "Susan Surovik", "type": "No asset", "links": []},
    491: {"name": "Brett Shead", "type": "No asset", "links": []},
    490: {"name": "Dan Jane", "type": "Google Drive video file",
          "links": ["https://drive.google.com/file/d/1LRw22Qv0RS-12vJ61PauCWQHGaga7JEd/view"]},
    489: {"name": "Jeremy May", "type": "Google Doc → YouTube video",
          "links": ["https://youtu.be/d6IR17a0M2o"]},
    488: {"name": "Olivia Tomlinson", "type": "Google Doc → 7 YouTube videos",
          "links": [
              "https://www.youtube.com/watch?v=NwS2ncgtkoc",
              "https://youtu.be/8zo0I4-F3Bs",
              "https://youtu.be/Dnmff9nv1b4",
              "https://youtu.be/2iwahDWerSQ",
              "https://www.youtube.com/watch?v=031Nbfiw4Q",
              "https://youtu.be/fiGmuUEOTPB",
              "https://youtu.be/3cU7aYwB9Lk"
          ]},
    487: {"name": "Shelesea Evans", "type": "Google Doc → Google Drive video link",
          "links": ["https://drive.google.com/file/d/1_dvbrXDDTlEYMGLuQ3ROsOxbAe2Riz/view"]},
    486: {"name": "Brandon Donahue", "type": "No asset", "links": []},
    485: {"name": "Emilie", "type": "Google Doc → Google Drive folder",
          "links": ["https://drive.google.com/drive/folders/1nrNku95GdWxGmfawSi6gLNb9Jaji_2r"]},
    484: {"name": "Taro", "type": "No asset", "links": []},
    483: {"name": "Brandon Donahue", "type": "Google Doc → YouTube video",
          "links": ["https://youtu.be/x2ejX4YbrA"]},
    482: {"name": "Joseph Cortone", "type": "Google Doc → 2 YouTube videos",
          "links": [
              "https://www.youtube.com/watch?v=QxVX2_B3hHs",
              "https://www.youtube.com/watch?v=Gytq7F2qgSY"
          ]},
    481: {"name": "Austyn Brown", "type": "Google Doc → YouTube video",
          "links": ["https://youtu.be/lnh9C65np68"]},
    480: {"name": "ISTPS", "type": "No asset", "links": []},
    479: {"name": "Taryn Hanes", "type": "Google Doc (deleted/unavailable) – no links", "links": []},
    478: {"name": "Shelly Chen", "type": "Google Doc (no video links)", "links": []},
    477: {"name": "Patryk Makara", "type": "No asset", "links": []},
    476: {"name": "Michele Q", "type": "No asset", "links": []},
    475: {"name": "Brenden Ohlsson", "type": "Google Doc → Google Drive video file",
          "links": ["https://drive.google.com/file/d/1u4vS-_WKpW-RHCpIS4hls6K5grOii56F/view"]},
    474: {"name": "Nathalie Bauer", "type": "Google Doc → YouTube video",
          "links": ["https://youtu.be/kK68tiL-RMo"]},
    473: {"name": "Kaioxx DarkMacro", "type": "No asset", "links": []},
    472: {"name": "James Yu", "type": "Google Doc → YouTube video",
          "links": ["https://www.youtube.com/watch?v=LwIMd_XX5Ho"]}
}


def test_row(row_id, row_data):
    """Test a single row"""
    print(f"\n{'='*80}")
    print(f"Testing Row {row_id}: {row_data['name']}")
    print(f"Type: {row_data['type']}")
    print(f"Expected links: {len(row_data['links'])}")
    
    # Skip if no Google Doc or no links expected
    if row_data['type'] == "No asset" or not row_data['links']:
        print("⏭️  Skipping - No Google Doc links to test")
        return {
            'row_id': row_id,
            'name': row_data['name'],
            'skipped': True,
            'reason': 'No Google Doc or no expected links'
        }
    
    # For testing, we only have URLs for specific docs
    if not row_data.get('doc_url'):
        print("⏭️  Skipping - No document URL available for testing")
        return {
            'row_id': row_id,
            'name': row_data['name'],
            'skipped': True,
            'reason': 'No document URL available'
        }
    
    try:
        # Extract content
        print(f"🔗 Extracting from: {row_data['doc_url']}")
        start_time = time.time()
        
        html_content, doc_text = step3_scrape_doc_contents(row_data['doc_url'])
        links = step4_extract_links(html_content, doc_text)
        
        extraction_time = time.time() - start_time
        
        # Collect all found links
        found_links = []
        found_links.extend(links.get('youtube', []))
        found_links.extend(links.get('drive_files', []))
        found_links.extend(links.get('drive_folders', []))
        
        # Compare with expected
        matches = []
        missing = []
        extra = found_links.copy()
        
        for expected_link in row_data['links']:
            found = False
            for found_link in found_links:
                if compare_urls_for_truth(found_link, expected_link):
                    matches.append({
                        'expected': expected_link,
                        'found': found_link,
                        'exact': normalize_url_for_truth_comparison(found_link) == normalize_url_for_truth_comparison(expected_link)
                    })
                    if found_link in extra:
                        extra.remove(found_link)
                    found = True
                    break
            
            if not found:
                missing.append(expected_link)
        
        # Results
        success = len(missing) == 0
        result = {
            'row_id': row_id,
            'name': row_data['name'],
            'type': row_data['type'],
            'success': success,
            'extraction_time': extraction_time,
            'expected_count': len(row_data['links']),
            'found_count': len(found_links),
            'matches': len(matches),
            'missing': missing,
            'extra': extra,
            'match_details': matches
        }
        
        # Display results
        print(f"\n📊 Results:")
        print(f"   Expected: {len(row_data['links'])} links")
        print(f"   Found: {len(found_links)} links")
        print(f"   Matches: {len(matches)}")
        print(f"   Missing: {len(missing)}")
        print(f"   Status: {'✅ PASS' if success else '❌ FAIL'}")
        
        if matches:
            print(f"\n   ✅ Matched links:")
            for match in matches:
                status = "exact" if match['exact'] else "fuzzy"
                print(f"      {match['found']} ({status})")
        
        if missing:
            print(f"\n   ❌ Missing links:")
            for link in missing:
                print(f"      {link}")
        
        if extra:
            print(f"\n   ➕ Extra links found:")
            for link in extra:
                print(f"      {link}")
        
        return result
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return {
            'row_id': row_id,
            'name': row_data['name'],
            'error': str(e),
            'success': False
        }

def main():
    """Test all 30 rows"""
    print("🎯 COMPREHENSIVE TRUTH SOURCE TEST")
    print("Testing rows 501-472 against expected results")
    print("="*80)
    
    results = []
    tested_count = 0
    
    # Test each row
    for row_id in sorted(TRUTH_SOURCE.keys(), reverse=True):
        result = test_row(row_id, TRUTH_SOURCE[row_id])
        results.append(result)
        
        if not result.get('skipped'):
            tested_count += 1
            # Wait between tests
            time.sleep(3)
    
    # Cleanup
    cleanup_selenium_driver()
    
    # Summary
    print(f"\n{'='*80}")
    print("📊 FINAL SUMMARY")
    print(f"{'='*80}")
    
    # Calculate statistics
    total_rows = len(results)
    skipped = sum(1 for r in results if r.get('skipped'))
    tested = sum(1 for r in results if not r.get('skipped'))
    successful = sum(1 for r in results if r.get('success'))
    failed = tested - successful
    
    print(f"\nTotal rows: {total_rows}")
    print(f"Tested: {tested}")
    print(f"Skipped: {skipped}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    
    if tested > 0:
        print(f"Success rate: {(successful/tested*100):.1f}%")
    
    print("\n📋 DETAILED RESULTS:")
    
    # Show tested results
    print("\nTESTED ROWS:")
    for result in results:
        if not result.get('skipped'):
            status = "✅" if result.get('success') else "❌"
            print(f"{status} Row {result['row_id']}: {result['name']}")
            print(f"   Expected: {result.get('expected_count', '?')} links")
            print(f"   Found: {result.get('found_count', '?')} links")
            print(f"   Matches: {result.get('matches', '?')}")
            
            if result.get('missing'):
                print(f"   Missing: {len(result['missing'])} links")
            if result.get('error'):
                print(f"   Error: {result['error']}")
    
    # Save results
    with open('all_30_rows_test_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n💾 Complete results saved to all_30_rows_test_results.json")
    
    # Analysis
    print("\n💡 ANALYSIS:")
    if tested > 0:
        if successful == tested:
            print("✅ PERFECT! All tested rows match expected results.")
        else:
            print("⚠️  Some discrepancies found:")
            for result in results:
                if not result.get('skipped') and not result.get('success'):
                    print(f"\n   Row {result['row_id']} ({result['name']}):")
                    if result.get('missing'):
                        print(f"     Missing {len(result['missing'])} links")
                    if result.get('extra'):
                        print(f"     Found {len(result['extra'])} extra links")
    
    return results

if __name__ == "__main__":
    main()