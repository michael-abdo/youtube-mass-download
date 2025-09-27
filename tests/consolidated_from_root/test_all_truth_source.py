#!/usr/bin/env python3
"""
TEST ALL TRUTH SOURCE WITH AUTO-EXTRACTED URLS
Uses the automatically extracted Google Doc URLs to test all available cases
"""

from utils.path_setup import init_project_imports
init_project_imports()

from simple_workflow import step1_download_sheet, step2_extract_people_and_docs, step3_scrape_doc_contents, step4_extract_links, cleanup_selenium_driver
from utils.patterns import normalize_url_for_truth_comparison, compare_urls_for_truth
import json
import time
import re

# Truth source data (rows 501-472)
TRUTH_SOURCE = {
    501: {"name": "Dmitriy Golovko", "expected_links": []},
    500: {"name": "Seth Dossett", "expected_links": []},
    499: {"name": "Carlos Arthur", "expected_links": ["https://www.youtube.com/watch?v=UD2X2hJTq4Y"]},
    498: {"name": "Caroline Chiu", "expected_links": ["https://drive.google.com/file/d/1c13knvRxfF-HrQhyBOfGIZpcrY-OR3_A/view"]},
    497: {"name": "James Kirton", "expected_links": [
        "https://youtube.com/playlist?list=PLp0u93QMy5vCYzE4OQydGEMEJjRizGDA",
        "https://youtube.com/playlist?list=PLu9i8x5U9PHhmD9K-5WY4EB12vyhL"
    ]},
    496: {"name": "Florence", "expected_links": []},
    495: {"name": "John Williams", "expected_links": [
        "https://www.youtube.com/watch?v=K6kBTbjH4cI",
        "https://youtu.be/vHD2wDyrWLw", 
        "https://youtu.be/BlSxvQ9p8Q0",
        "https://youtu.be/ZBuf3DGBuM"
    ]},
    494: {"name": "Maddie Boyle", "expected_links": ["https://www.youtube.com/watch?v=4HfkyhvaaWc"]},
    493: {"name": "Kiko", "expected_links": ["https://drive.google.com/drive/folders/1-4mmoEIuZKq4xOuJpzeBdnxbkYZQJZTl"]},
    492: {"name": "Susan Surovik", "expected_links": []},
    491: {"name": "Brett Shead", "expected_links": []},
    490: {"name": "Dan Jane", "expected_links": ["https://drive.google.com/file/d/1LRw22Qv0RS-12vJ61PauCWQHGaga7JEd/view"]},
    489: {"name": "Jeremy May", "expected_links": ["https://youtu.be/d6IR17a0M2o"]},
    488: {"name": "Olivia Tomlinson", "expected_links": [
        "https://www.youtube.com/watch?v=NwS2ncgtkoc",
        "https://youtu.be/8zo0I4-F3Bs",
        "https://youtu.be/Dnmff9nv1b4", 
        "https://youtu.be/2iwahDWerSQ",
        "https://www.youtube.com/watch?v=031Nbfiw4Q",
        "https://youtu.be/fiGmuUEOTPB",
        "https://youtu.be/3cU7aYwB9Lk"
    ]},
    487: {"name": "Shelesea Evans", "expected_links": ["https://drive.google.com/file/d/1_dvbrXDDTlEYMGLuQ3ROsOxbAe2Riz/view"]},
    486: {"name": "Brandon Donahue", "expected_links": []},
    485: {"name": "Emilie", "expected_links": ["https://drive.google.com/drive/folders/1nrNku95GdWxGmfawSi6gLNb9Jaji_2r"]},
    484: {"name": "Taro", "expected_links": []},
    483: {"name": "Brandon Donahue", "expected_links": ["https://youtu.be/x2ejX4YbrA"]},
    482: {"name": "Joseph Cortone", "expected_links": [
        "https://www.youtube.com/watch?v=QxVX2_B3hHs",
        "https://www.youtube.com/watch?v=Gytq7F2qgSY"
    ]},
    481: {"name": "Austyn Brown", "expected_links": ["https://youtu.be/lnh9C65np68"]},
    480: {"name": "ISTPS", "expected_links": []},
    479: {"name": "Taryn Hanes", "expected_links": []},
    478: {"name": "Shelly Chen", "expected_links": []},
    477: {"name": "Patryk Makara", "expected_links": []},
    476: {"name": "Michele Q", "expected_links": []},
    475: {"name": "Brenden Ohlsson", "expected_links": ["https://drive.google.com/file/d/1u4vS-_WKpW-RHCpIS4hls6K5grOii56F/view"]},
    474: {"name": "Nathalie Bauer", "expected_links": ["https://youtu.be/kK68tiL-RMo"]},
    473: {"name": "Kaioxx DarkMacro", "expected_links": []},
    472: {"name": "James Yu", "expected_links": ["https://www.youtube.com/watch?v=LwIMd_XX5Ho"]}
}


def test_person_extraction(person_data, expected_data):
    """Test extraction for a specific person"""
    name = person_data['name']
    row_id = person_data['row_id']
    doc_url = person_data['doc_link']
    expected_links = expected_data['expected_links']
    
    print(f"\n{'='*80}")
    print(f"Testing Row {row_id}: {name}")
    print(f"Expected links: {len(expected_links)}")
    
    # Skip if no doc URL or no expected links
    if not doc_url:
        if expected_links:
            return {
                'row_id': row_id,
                'name': name,
                'success': False,
                'reason': 'No Google Doc URL found but links expected',
                'expected_count': len(expected_links),
                'found_count': 0
            }
        else:
            return {
                'row_id': row_id,
                'name': name,
                'success': True,
                'reason': 'No doc URL and no links expected',
                'skipped': True
            }
    
    if not expected_links:
        print("⏭️  Skipping - No links expected from this document")
        return {
            'row_id': row_id,
            'name': name,
            'success': True,
            'reason': 'Has doc but no links expected',
            'skipped': True
        }
    
    try:
        print(f"🔗 Extracting from: {doc_url[:80]}...")
        start_time = time.time()
        
        # Extract content and links
        html_content, doc_text = step3_scrape_doc_contents(doc_url)
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
        
        for expected_link in expected_links:
            found = False
            for found_link in found_links:
                if compare_urls_for_truth(found_link, expected_link):
                    matches.append({
                        'expected': expected_link,
                        'found': found_link
                    })
                    found = True
                    break
            
            if not found:
                missing.append(expected_link)
        
        # Results
        success = len(missing) == 0
        result = {
            'row_id': row_id,
            'name': name,
            'success': success,
            'extraction_time': extraction_time,
            'expected_count': len(expected_links),
            'found_count': len(found_links),
            'matches': len(matches),
            'missing': missing,
            'found_links': found_links,
            'expected_links': expected_links
        }
        
        # Display results
        print(f"📊 Results:")
        print(f"   Expected: {len(expected_links)} links")
        print(f"   Found: {len(found_links)} links")
        print(f"   Matches: {len(matches)}")
        print(f"   Status: {'✅ PASS' if success else '❌ FAIL'}")
        
        if matches:
            print(f"   ✅ Matched links:")
            for match in matches:
                print(f"      {match['found']}")
        
        if missing:
            print(f"   ❌ Missing links:")
            for link in missing:
                print(f"      {link}")
        
        return result
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return {
            'row_id': row_id,
            'name': name,
            'error': str(e),
            'success': False
        }

def main():
    """Test all truth source rows using auto-extracted URLs"""
    print("🎯 COMPREHENSIVE TRUTH SOURCE TEST WITH AUTO-EXTRACTED URLS")
    print("="*80)
    
    # Get all people data with Google Doc URLs
    print("1. Getting all people with Google Doc URLs...")
    html_content = step1_download_sheet()
    people_data, people_with_docs = step2_extract_people_and_docs(html_content)
    
    print(f"   Total people: {len(people_data)}")
    print(f"   People with docs: {len(people_with_docs)}")
    
    # Create lookup by name for auto-extracted data
    people_lookup = {person['name']: person for person in people_data}
    
    # Test each person from truth source
    results = []
    tested_count = 0
    
    print(f"\n2. Testing truth source rows (501-472)...")
    
    for row_id in sorted(TRUTH_SOURCE.keys(), reverse=True):
        expected_data = TRUTH_SOURCE[row_id]
        name = expected_data['name']
        
        # Find the person in our extracted data
        if name in people_lookup:
            person_data = people_lookup[name]
            result = test_person_extraction(person_data, expected_data)
            results.append(result)
            
            if not result.get('skipped'):
                tested_count += 1
                # Wait between tests to avoid rate limiting
                time.sleep(2)
        else:
            print(f"\n⚠️  Row {row_id}: {name} - NOT FOUND in extracted data")
            results.append({
                'row_id': row_id,
                'name': name,
                'success': False,
                'reason': 'Person not found in extracted data'
            })
    
    # Cleanup
    cleanup_selenium_driver()
    
    # Generate summary
    print(f"\n{'='*80}")
    print("📊 FINAL RESULTS")
    print(f"{'='*80}")
    
    # Calculate statistics
    total_rows = len(results)
    skipped = sum(1 for r in results if r.get('skipped'))
    tested = sum(1 for r in results if not r.get('skipped') and not r.get('reason', '').startswith('Person not found'))
    successful = sum(1 for r in results if r.get('success') and not r.get('skipped'))
    failed = tested - successful
    
    print(f"\nStatistics:")
    print(f"   Total truth source rows: {total_rows}")
    print(f"   Successfully tested: {successful}")
    print(f"   Failed tests: {failed}")
    print(f"   Skipped (no links expected): {skipped}")
    print(f"   Not found in data: {total_rows - tested - skipped}")
    
    if tested > 0:
        success_rate = (successful / tested) * 100
        print(f"   Success rate: {success_rate:.1f}%")
    
    # Detailed results
    print(f"\n📋 DETAILED RESULTS:")
    
    for result in results:
        if result.get('skipped'):
            print(f"⏭️  Row {result['row_id']}: {result['name']} - SKIPPED")
        elif result.get('success'):
            print(f"✅ Row {result['row_id']}: {result['name']} - PASS ({result.get('matches', 0)}/{result.get('expected_count', 0)})")
        else:
            status = result.get('reason', 'FAIL')
            print(f"❌ Row {result['row_id']}: {result['name']} - {status}")
            if result.get('missing'):
                print(f"     Missing: {len(result['missing'])} links")
    
    # Save complete results
    with open('comprehensive_truth_source_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n💾 Complete results saved to comprehensive_truth_source_results.json")
    
    # Success analysis
    if tested > 0:
        if successful == tested:
            print(f"\n🎉 PERFECT SUCCESS! All {tested} testable rows passed extraction!")
        else:
            print(f"\n⚠️  {failed} out of {tested} tests failed. Check individual results for details.")
    
    return results

if __name__ == "__main__":
    main()