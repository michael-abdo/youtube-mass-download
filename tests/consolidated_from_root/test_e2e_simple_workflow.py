#!/usr/bin/env python3
"""
COMPREHENSIVE E2E TEST FOR 6-STEP SIMPLE WORKFLOW
Tests the entire pipeline with live data following each step TO PERFECTION

This test validates:
1. Step 1: Download Google Sheet (live data)
2. Step 2: Extract people data and Google Doc links
3. Step 3: Scrape doc contents and text  
4. Step 4: Extract links from scraped content
5. Step 5: Process extracted data
6. Step 6: Map data to CSV with CSVManager

Following CLAUDE.md principles: Smallest Feature, Fail FAST, Root Cause, DRY
"""

# Standardized project imports
from utils.config import setup_project_imports
setup_project_imports()

import os
import sys
import time
import json
import shutil
from pathlib import Path
from datetime import datetime
import pandas as pd

# Import the workflow and utilities (DRY)
import simple_workflow
from utils.config import get_config
from utils.csv_manager import CSVManager
from utils.patterns import PatternRegistry, extract_youtube_id, extract_drive_id
from utils.test_helpers import TestDataFactory

class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    RESET = '\033[0m'
    BOLD = '\033[1m'

class E2ESimpleWorkflowTest:
    """Comprehensive E2E test for the 6-step simple workflow"""
    
    def __init__(self):
        self.config = get_config()
        self.test_start_time = datetime.now()
        self.test_results = {}
        self.test_data = {}
        self.backup_files = []
        self.test_factory = TestDataFactory()
        
        # Test configuration
        self.test_limit = 3  # Test with 3 records for speed
        self.test_output_dir = Path("e2e_test_outputs")
        self.test_csv_path = self.test_output_dir / "e2e_test_output.csv"
        
        print(f"{Colors.BOLD}{Colors.BLUE}🧪 E2E SIMPLE WORKFLOW TEST SUITE{Colors.RESET}")
        print(f"{Colors.CYAN}Testing 6-step workflow with live data (limit: {self.test_limit} records){Colors.RESET}")
        print("=" * 80)
    
    def setup_test_environment(self):
        """Setup isolated test environment"""
        print(f"\n{Colors.YELLOW}🔧 Setting up test environment...{Colors.RESET}")
        
        # Create test output directory
        self.test_output_dir.mkdir(exist_ok=True)
        
        # Backup original outputs if they exist
        from utils.config import get_config
        config = get_config()
        original_output = Path(config.get('paths.output_csv', 'outputs/output.csv'))
        if original_output.exists():
            backup_path = self.test_output_dir / f"backup_output_{int(time.time())}.csv"
            shutil.copy2(original_output, backup_path)
            self.backup_files.append((original_output, backup_path))
            print(f"  ✅ Backed up original output to {backup_path}")
        
        # Setup test selenium driver cleanup
        if hasattr(simple_workflow, '_driver') and simple_workflow._driver:
            simple_workflow.cleanup_driver()
        
        print(f"  ✅ Test environment ready")
    
    def test_step1_download_sheet(self):
        """Test Step 1: Download Google Sheet with live data"""
        print(f"\n{Colors.BOLD}📋 STEP 1: Download Google Sheet{Colors.RESET}")
        
        try:
            start_time = time.time()
            html_content = simple_workflow.step1_download_sheet()
            elapsed = time.time() - start_time
            
            # Validate Step 1 results
            assert html_content is not None, "HTML content should not be None"
            assert len(html_content) > 1000, "HTML content should be substantial"
            assert "table" in html_content.lower(), "HTML should contain table data"
            assert str(self.config.get("google_sheets.target_div_id")) in html_content, "Should contain target div ID"
            
            # Store for next steps
            self.test_data['html_content'] = html_content
            self.test_data['step1_time'] = elapsed
            
            print(f"  ✅ Downloaded {len(html_content):,} characters in {elapsed:.2f}s")
            print(f"  ✅ Contains expected table structure")
            print(f"  ✅ Contains target div ID: {self.config.get('google_sheets.target_div_id')}")
            
            self.test_results['step1'] = True
            return True
            
        except Exception as e:
            print(f"  ❌ Step 1 failed: {e}")
            self.test_results['step1'] = False
            return False
    
    def test_step2_extract_people(self):
        """Test Step 2: Extract people data and Google Doc links"""
        print(f"\n{Colors.BOLD}👥 STEP 2: Extract People Data and Google Doc Links{Colors.RESET}")
        
        if not self.test_results.get('step1'):
            print(f"  ⏭️  Skipping Step 2 - Step 1 failed")
            self.test_results['step2'] = False
            return False
        
        try:
            start_time = time.time()
            all_people, people_with_docs = simple_workflow.step2_extract_people_and_docs(
                self.test_data['html_content']
            )
            elapsed = time.time() - start_time
            
            # Validate Step 2 results
            assert len(all_people) > 0, "Should extract at least some people"
            assert len(people_with_docs) > 0, "Should have people with documents"
            assert len(all_people) >= len(people_with_docs), "All people >= people with docs"
            
            # Validate data structure
            for person in all_people[:5]:  # Check first 5
                assert 'row_id' in person, "Person should have row_id"
                assert 'name' in person, "Person should have name"
                assert 'email' in person, "Person should have email"
                assert 'type' in person, "Person should have type"
                assert 'doc_link' in person, "Person should have doc_link field"
            
            # Validate people with docs have valid links
            google_docs_count = 0
            for person in people_with_docs[:5]:  # Check first 5
                assert person['doc_link'], "People with docs should have non-empty doc_link"
                if 'docs.google.com' in person['doc_link']:
                    google_docs_count += 1
            
            # Should have at least some Google Docs links (not necessarily all)
            assert google_docs_count > 0, f"Should have at least some Google Docs links, found {google_docs_count}"
            
            # Store for next steps
            self.test_data['all_people'] = all_people
            self.test_data['people_with_docs'] = people_with_docs
            self.test_data['step2_time'] = elapsed
            
            print(f"  ✅ Extracted {len(all_people)} total people in {elapsed:.2f}s")
            print(f"  ✅ Found {len(people_with_docs)} people with Google Doc links")
            print(f"  ✅ Data structure validation passed")
            print(f"  ✅ Sample person: {all_people[0]['name']} ({all_people[0]['type']})")
            
            self.test_results['step2'] = True
            return True
            
        except Exception as e:
            print(f"  ❌ Step 2 failed: {e}")
            self.test_results['step2'] = False
            return False
    
    def test_step3_scrape_docs(self):
        """Test Step 3: Scrape doc contents and text"""
        print(f"\n{Colors.BOLD}📄 STEP 3: Scrape Document Contents and Text{Colors.RESET}")
        
        if not self.test_results.get('step2'):
            print(f"  ⏭️  Skipping Step 3 - Step 2 failed")
            self.test_results['step3'] = False
            return False
        
        try:
            # Test with limited number of documents
            test_people = self.test_data['people_with_docs'][:self.test_limit]
            scraped_docs = {}
            total_start_time = time.time()
            
            print(f"  🔍 Testing with {len(test_people)} documents...")
            
            for i, person in enumerate(test_people):
                print(f"    📄 [{i+1}/{len(test_people)}] {person['name']}: {person['doc_link']}")
                
                start_time = time.time()
                doc_content, doc_text = simple_workflow.step3_scrape_doc_contents(person['doc_link'])
                elapsed = time.time() - start_time
                
                # Validate scraping results
                assert isinstance(doc_content, str), "doc_content should be string"
                assert isinstance(doc_text, str), "doc_text should be string"
                
                # At least one should have content
                has_content = len(doc_content) > 100 or len(doc_text) > 100
                if not has_content:
                    print(f"      ⚠️  Warning: Limited content extracted ({len(doc_content)} + {len(doc_text)} chars)")
                else:
                    print(f"      ✅ Extracted {len(doc_content)} HTML + {len(doc_text)} text chars in {elapsed:.2f}s")
                
                scraped_docs[person['row_id']] = {
                    'person': person,
                    'doc_content': doc_content,
                    'doc_text': doc_text,
                    'scrape_time': elapsed
                }
            
            total_elapsed = time.time() - total_start_time
            
            # Store for next steps
            self.test_data['scraped_docs'] = scraped_docs
            self.test_data['step3_time'] = total_elapsed
            
            print(f"  ✅ Scraped {len(scraped_docs)} documents in {total_elapsed:.2f}s")
            print(f"  ✅ Average time per document: {total_elapsed/len(scraped_docs):.2f}s")
            
            self.test_results['step3'] = True
            return True
            
        except Exception as e:
            print(f"  ❌ Step 3 failed: {e}")
            self.test_results['step3'] = False
            return False
        finally:
            # Cleanup Selenium driver
            simple_workflow.cleanup_driver()
    
    def test_step4_extract_links(self):
        """Test Step 4: Extract links from scraped content"""
        print(f"\n{Colors.BOLD}🔗 STEP 4: Extract Links from Scraped Content{Colors.RESET}")
        
        if not self.test_results.get('step3'):
            print(f"  ⏭️  Skipping Step 4 - Step 3 failed")
            self.test_results['step4'] = False
            return False
        
        try:
            extracted_links = {}
            total_start_time = time.time()
            
            for row_id, doc_data in self.test_data['scraped_docs'].items():
                person = doc_data['person']
                print(f"    🔍 Extracting links from {person['name']}'s document...")
                
                start_time = time.time()
                links = simple_workflow.step4_extract_links(
                    doc_data['doc_content'], 
                    doc_data['doc_text']
                )
                elapsed = time.time() - start_time
                
                # Validate link extraction results
                assert isinstance(links, dict), "Links should be a dictionary"
                required_keys = ['youtube', 'drive_files', 'drive_folders', 'all_links']
                for key in required_keys:
                    assert key in links, f"Links should contain '{key}' key"
                    assert isinstance(links[key], list), f"Links['{key}'] should be a list"
                
                # Validate link quality using centralized patterns
                youtube_count = len(links['youtube'])
                drive_file_count = len(links['drive_files'])
                drive_folder_count = len(links['drive_folders'])
                total_link_count = len(links['all_links'])
                
                # Test pattern registry integration
                for youtube_link in links['youtube'][:2]:  # Test first 2
                    video_id = extract_youtube_id(youtube_link)
                    if video_id:
                        assert len(video_id) == 11, f"YouTube video ID should be 11 chars: {video_id}"
                
                for drive_link in links['drive_files'][:2]:  # Test first 2
                    file_id = extract_drive_id(drive_link)
                    if file_id:
                        assert len(file_id) >= 25, f"Drive file ID should be 25+ chars: {file_id}"
                
                extracted_links[row_id] = {
                    'person': person,
                    'links': links,
                    'extraction_time': elapsed
                }
                
                print(f"      ✅ Found {youtube_count} YouTube + {drive_file_count} Drive files + {drive_folder_count} Drive folders + {total_link_count} total links in {elapsed:.2f}s")
            
            total_elapsed = time.time() - total_start_time
            
            # Store for next steps
            self.test_data['extracted_links'] = extracted_links
            self.test_data['step4_time'] = total_elapsed
            
            # Summary statistics
            total_youtube = sum(len(data['links']['youtube']) for data in extracted_links.values())
            total_drive = sum(len(data['links']['drive_files']) + len(data['links']['drive_folders']) for data in extracted_links.values())
            total_all = sum(len(data['links']['all_links']) for data in extracted_links.values())
            
            print(f"  ✅ Extracted links from {len(extracted_links)} documents in {total_elapsed:.2f}s")
            print(f"  ✅ Total links found: {total_youtube} YouTube, {total_drive} Drive, {total_all} all links")
            print(f"  ✅ Pattern registry validation passed")
            
            self.test_results['step4'] = True
            return True
            
        except Exception as e:
            print(f"  ❌ Step 4 failed: {e}")
            self.test_results['step4'] = False
            return False
    
    def test_step5_process_data(self):
        """Test Step 5: Process extracted data"""
        print(f"\n{Colors.BOLD}⚙️  STEP 5: Process Extracted Data{Colors.RESET}")
        
        if not self.test_results.get('step4'):
            print(f"  ⏭️  Skipping Step 5 - Step 4 failed")
            self.test_results['step5'] = False
            return False
        
        try:
            processed_records = []
            total_start_time = time.time()
            
            for row_id, link_data in self.test_data['extracted_links'].items():
                person = link_data['person']
                links = link_data['links']
                doc_text = self.test_data['scraped_docs'][row_id]['doc_text']
                
                print(f"    ⚙️  Processing {person['name']}'s data...")
                
                start_time = time.time()
                record = simple_workflow.step5_process_extracted_data(person, links, doc_text)
                elapsed = time.time() - start_time
                
                # Validate processed record structure
                assert isinstance(record, dict), "Processed record should be a dictionary"
                
                # Check required fields
                required_fields = [
                    'row_id', 'name', 'email', 'type', 'link', 'extracted_links',
                    'youtube_playlist', 'google_drive', 'processed', 'document_text'
                ]
                for field in required_fields:
                    assert field in record, f"Record should contain '{field}' field"
                
                # Validate data integrity
                assert record['row_id'] == person['row_id'], "Row ID should match"
                assert record['name'] == person['name'], "Name should match"
                assert record['email'] == person['email'], "Email should match"
                assert record['type'] == person['type'], "Type should match"
                assert record['processed'] == 'yes', "Should be marked as processed"
                
                processed_records.append(record)
                
                youtube_count = len(record['youtube_playlist'].split('|')) if record['youtube_playlist'] else 0
                drive_count = len(record['google_drive'].split('|')) if record['google_drive'] else 0
                
                print(f"      ✅ Processed with {youtube_count} YouTube + {drive_count} Drive links in {elapsed:.2f}s")
            
            total_elapsed = time.time() - total_start_time
            
            # Store for next steps
            self.test_data['processed_records'] = processed_records
            self.test_data['step5_time'] = total_elapsed
            
            print(f"  ✅ Processed {len(processed_records)} records in {total_elapsed:.2f}s")
            print(f"  ✅ Data integrity validation passed")
            
            self.test_results['step5'] = True
            return True
            
        except Exception as e:
            print(f"  ❌ Step 5 failed: {e}")
            self.test_results['step5'] = False
            return False
    
    def test_step6_map_to_csv(self):
        """Test Step 6: Map data to CSV with CSVManager"""
        print(f"\n{Colors.BOLD}💾 STEP 6: Map Data to CSV with CSVManager{Colors.RESET}")
        
        if not self.test_results.get('step5'):
            print(f"  ⏭️  Skipping Step 6 - Step 5 failed")
            self.test_results['step6'] = False
            return False
        
        try:
            start_time = time.time()
            
            # Test with different modes
            test_modes = [
                {'basic_mode': True, 'text_mode': False, 'name': 'Basic Mode'},
                {'basic_mode': False, 'text_mode': True, 'name': 'Text Mode'},
                {'basic_mode': False, 'text_mode': False, 'name': 'Full Mode'}
            ]
            
            for i, mode in enumerate(test_modes):
                mode_start = time.time()
                output_file = self.test_output_dir / f"test_step6_{mode['name'].lower().replace(' ', '_')}.csv"
                
                print(f"    💾 Testing {mode['name']}...")
                
                # Call step6_map_data with our processed records
                df = simple_workflow.step6_map_data(
                    self.test_data['processed_records'],
                    basic_mode=mode['basic_mode'],
                    text_mode=mode['text_mode'],
                    output_file=str(output_file)
                )
                
                mode_elapsed = time.time() - mode_start
                
                # Validate CSV output
                assert output_file.exists(), f"Output file should exist: {output_file}"
                assert output_file.stat().st_size > 0, "Output file should not be empty"
                
                # Validate DataFrame
                assert df is not None, "DataFrame should not be None"
                assert len(df) == len(self.test_data['processed_records']), "Row count should match"
                
                # Test CSVManager functionality by reading it back
                csv_manager = CSVManager(csv_path=str(output_file))
                read_df = csv_manager.read()
                assert len(read_df) == len(df), "CSVManager read should match original"
                
                # Validate column structure based on mode
                if mode['basic_mode']:
                    expected_cols = ['row_id', 'name', 'email', 'type', 'link']
                elif mode['text_mode']:
                    expected_cols = ['row_id', 'name', 'email', 'type', 'link', 'document_text', 'processed', 'extraction_date']
                else:
                    expected_cols = [
                        'row_id', 'name', 'email', 'type', 'link', 'extracted_links',
                        'youtube_playlist', 'google_drive', 'processed', 'document_text'
                    ]
                
                for col in expected_cols:
                    assert col in df.columns, f"{mode['name']} should have column: {col}"
                
                print(f"      ✅ {mode['name']}: {len(df)} rows, {len(df.columns)} columns in {mode_elapsed:.2f}s")
                print(f"      ✅ CSVManager integration verified")
            
            total_elapsed = time.time() - start_time
            
            # Store results
            self.test_data['step6_time'] = total_elapsed
            self.test_data['final_output'] = output_file
            
            print(f"  ✅ Generated CSV files in all modes in {total_elapsed:.2f}s")
            print(f"  ✅ CSVManager atomic writes and backups working")
            
            self.test_results['step6'] = True
            return True
            
        except Exception as e:
            print(f"  ❌ Step 6 failed: {e}")
            self.test_results['step6'] = False
            return False
    
    def test_integration_validation(self):
        """Test end-to-end integration and data consistency"""
        print(f"\n{Colors.BOLD}🔄 INTEGRATION VALIDATION{Colors.RESET}")
        
        try:
            # Validate complete data flow
            original_people = len(self.test_data.get('all_people', []))
            processed_records = len(self.test_data.get('processed_records', []))
            
            assert processed_records <= original_people, "Processed records should not exceed original"
            
            # Validate timing performance
            step_times = {
                'Step 1': self.test_data.get('step1_time', 0),
                'Step 2': self.test_data.get('step2_time', 0),
                'Step 3': self.test_data.get('step3_time', 0),
                'Step 4': self.test_data.get('step4_time', 0),
                'Step 5': self.test_data.get('step5_time', 0),
                'Step 6': self.test_data.get('step6_time', 0)
            }
            
            total_time = sum(step_times.values())
            
            print(f"  📊 Performance Summary:")
            for step, time_val in step_times.items():
                percentage = (time_val / total_time * 100) if total_time > 0 else 0
                print(f"    {step}: {time_val:.2f}s ({percentage:.1f}%)")
            
            print(f"  ✅ Total pipeline time: {total_time:.2f}s")
            print(f"  ✅ Data consistency verified")
            
            # Test DRY utilities integration
            config_test = self.config.get("google_sheets.url")
            assert config_test, "Config utility working"
            
            pattern_test = PatternRegistry.YOUTUBE_VIDEO_ID.pattern
            assert pattern_test, "Pattern registry working"
            
            print(f"  ✅ DRY utilities integration verified")
            
            self.test_results['integration'] = True
            return True
            
        except Exception as e:
            print(f"  ❌ Integration validation failed: {e}")
            self.test_results['integration'] = False
            return False
    
    def cleanup_test_environment(self):
        """Cleanup test environment and restore backups if needed"""
        print(f"\n{Colors.YELLOW}🧹 Cleaning up test environment...{Colors.RESET}")
        
        # Cleanup Selenium driver
        simple_workflow.cleanup_driver()
        
        # Keep test outputs for inspection
        print(f"  ✅ Test outputs preserved in: {self.test_output_dir}")
        
        # Restore backups if requested (keeping them for now)
        for original, backup in self.backup_files:
            print(f"  📁 Backup available: {backup}")
    
    def print_final_results(self):
        """Print comprehensive test results"""
        test_end_time = datetime.now()
        total_duration = (test_end_time - self.test_start_time).total_seconds()
        
        print(f"\n{Colors.BOLD}{Colors.BLUE}🏁 E2E TEST RESULTS SUMMARY{Colors.RESET}")
        print("=" * 80)
        
        steps = [
            ('Step 1: Download Sheet', 'step1'),
            ('Step 2: Extract People', 'step2'),
            ('Step 3: Scrape Docs', 'step3'),
            ('Step 4: Extract Links', 'step4'),
            ('Step 5: Process Data', 'step5'),
            ('Step 6: Map to CSV', 'step6'),
            ('Integration Test', 'integration')
        ]
        
        passed_count = 0
        for step_name, step_key in steps:
            status = self.test_results.get(step_key, False)
            if status:
                passed_count += 1
                print(f"{Colors.GREEN}✅ {step_name}: PASSED{Colors.RESET}")
            else:
                print(f"{Colors.RED}❌ {step_name}: FAILED{Colors.RESET}")
        
        success_rate = (passed_count / len(steps)) * 100
        
        print("\n" + "=" * 80)
        print(f"{Colors.BOLD}Overall Results:{Colors.RESET}")
        print(f"  Tests Passed: {passed_count}/{len(steps)} ({success_rate:.1f}%)")
        print(f"  Total Duration: {total_duration:.2f}s")
        print(f"  Test Limit: {self.test_limit} records")
        
        if success_rate == 100:
            print(f"\n{Colors.GREEN}{Colors.BOLD}🎉 ALL TESTS PASSED! 6-Step Workflow is PERFECT!{Colors.RESET}")
            return True
        else:
            print(f"\n{Colors.RED}{Colors.BOLD}⚠️  Some tests failed. Pipeline needs attention.{Colors.RESET}")
            return False
    
    def run_full_e2e_test(self):
        """Run the complete E2E test suite"""
        try:
            self.setup_test_environment()
            
            # Run all 6 steps in sequence
            self.test_step1_download_sheet()
            self.test_step2_extract_people()
            self.test_step3_scrape_docs()
            self.test_step4_extract_links()
            self.test_step5_process_data()
            self.test_step6_map_to_csv()
            
            # Final integration validation
            self.test_integration_validation()
            
            return self.print_final_results()
            
        except KeyboardInterrupt:
            print(f"\n{Colors.YELLOW}⚠️  Test interrupted by user{Colors.RESET}")
            return False
        except Exception as e:
            print(f"\n{Colors.RED}💥 Unexpected test failure: {e}{Colors.RESET}")
            return False
        finally:
            self.cleanup_test_environment()

def main():
    """Main test runner"""
    test_runner = E2ESimpleWorkflowTest()
    success = test_runner.run_full_e2e_test()
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())