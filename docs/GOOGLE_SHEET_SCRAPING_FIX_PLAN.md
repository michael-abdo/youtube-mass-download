# Google Sheet Scraping Fix Implementation Plan

## Executive Summary

The current scraping system is **missing 7 people** due to hardcoded row indexing and has **massive data duplication** (7,893 rows for 472 unique people). The fix requires updating the scraping logic, implementing row ID-based duplicate detection, and enhancing data integrity.

## Affected Components Analysis

### 1. Primary Files to Modify
- `utils/scrape_google_sheets.py` - Core scraping logic (fetch_table_data, update_csv)
- `utils/atomic_csv.py` - CSV writing operations 
- `config.yaml` - Configuration for new row_id tracking
- `output.csv` - Schema changes (add row_id column)

### 2. Secondary Files Impacted
- `utils/master_scraper.py` - Orchestration layer
- `run_complete_workflow.py` - Pipeline integration
- `utils/extract_links.py` - Link processing on new data
- `tests/` - New test coverage needed

### 3. Data Dependencies
- Google Sheet HTML structure (div ID 1159146182)
- CSV field structure and column ordering
- Backup system (`backups/output/` directory)
- File locking mechanisms

### 4. Configuration Dependencies
- `google_sheets.target_div_id` - HTML parsing
- `paths.output_csv` - File location
- `google_sheets.cache_file` - HTML caching

## Root Cause Analysis

1. **Hardcoded Row 15**: Line 246 in `scrape_google_sheets.py` skips first 14 rows
2. **Link-Only Duplicates**: Only checks URL existence, not person identity
3. **No Row ID Usage**: Ignores natural unique identifier in column 0
4. **Multiple Execution**: No prevention of duplicate runs adding same data

## Implementation Strategy (Simplest Approach)

**Phase 1: Immediate Fix (Zero Downtime)**
- Fix row indexing to capture missing entries
- Add row_id tracking without changing CSV schema
- Implement better duplicate detection

**Phase 2: Schema Enhancement (Requires Migration)**
- Add row_id column to CSV
- Migrate existing data
- Enhanced duplicate resolution

## Detailed Step-by-Step Plan

### Step 1: Pre-Implementation Analysis
1. **Backup Current State**
   - Create full backup of `output.csv`
   - Document current row counts and unique identifiers
   - Test current scraping to establish baseline

2. **Analyze Missing Data**
   - Identify the 7 missing people (rows 7-14)
   - Verify they have valid document links
   - Confirm they're not duplicates of existing entries

### Step 2: Update Scraping Logic (Core Fix)

**File: `utils/scrape_google_sheets.py`**

1. **Fix Row Start Index**
   - Location: Line 246 `for row_index in range(15, len(rows))`
   - Change to: `for row_index in range(1, len(rows))`
   - Rationale: Row 0 is empty, Row 1 is header, data starts at Row 2

2. **Add Row ID Extraction**
   - Extract `row_id` from `cells[0].get_text(strip=True)` 
   - Store in record dictionary as `"row_id": row_id`
   - This captures the Google Sheet's natural unique identifier

3. **Enhanced Record Structure**
   ```python
   record = {
       "name": name,
       "email": email, 
       "type": type_val,
       "link": doc_link,
       "row_id": row_id  # NEW FIELD
   }
   ```

### Step 3: Implement Row ID-Based Duplicate Detection

**File: `utils/scrape_google_sheets.py` (update_csv function)**

1. **Create Row ID Tracking Set**
   - Read existing CSV and build set of processed row_ids
   - Use this as primary duplicate detection mechanism
   - Fallback to link-based detection for legacy compatibility

2. **Duplicate Detection Logic**
   ```python
   # Primary: Check row_id (most reliable)
   if row_id in existing_row_ids:
       continue
   
   # Secondary: Check link (legacy compatibility) 
   if link in existing_links:
       continue
       
   # Add to both tracking sets
   existing_row_ids.add(row_id)
   existing_links.add(link)
   ```

### Step 4: Update CSV Schema (Minimal Impact)

**Approach: Soft Migration**
1. **Modify `fieldnames` in `write_csv_atomic`/`append_csv_atomic`**
   - Add `"row_id"` to fieldnames list
   - Existing data without row_id will have empty values
   - New data will populate row_id field

2. **Backward Compatibility**
   - Existing CSV reading code continues to work
   - row_id field is optional for existing entries
   - Only new entries require row_id

### Step 5: Update Atomic CSV Operations

**File: `utils/atomic_csv.py`**

1. **Verify Fieldname Handling**
   - Ensure `row_id` field is properly included in writes
   - Test atomic operations with new schema
   - Maintain file locking and backup behavior

### Step 6: Configuration Updates

**File: `config.yaml`**
1. **Add New Configuration**
   ```yaml
   google_sheets:
     use_row_id_deduplication: true
     start_row_index: 1  # Make configurable
   ```

### Step 7: Testing Strategy

1. **Unit Tests** (`tests/test_scraping.py` - NEW FILE)
   - Test row_id extraction
   - Test duplicate detection logic
   - Test CSV schema compatibility

2. **Integration Tests**
   - Test full scraping pipeline with new logic
   - Verify no data loss
   - Confirm duplicate prevention

3. **Data Validation Tests**
   - Run scraping on test data
   - Verify all 218 people with links are captured
   - Confirm no duplicates in output

### Step 8: Migration and Deployment

1. **Pre-Deployment Verification**
   - Run new scraping logic on cached HTML
   - Compare output with expected results
   - Verify the 7 missing people are captured

2. **Deployment Process**
   - Deploy during low-usage window
   - Create pre-deployment backup
   - Run single scraping cycle
   - Verify data integrity

3. **Post-Deployment Validation**
   - Verify CSV row count is reasonable (~471 unique people)
   - Confirm no data loss from existing entries
   - Test that subsequent runs don't create duplicates

### Step 9: Monitoring and Cleanup

1. **Add Logging**
   - Log row_id processing for debugging
   - Track duplicate detection statistics
   - Monitor CSV growth patterns

2. **Documentation Updates**
   - Update README with new duplicate detection logic
   - Document row_id field in CSV schema
   - Add troubleshooting guide

## Edge Cases and Risk Mitigation

### Edge Case 1: Row ID Changes
- **Risk**: Google Sheet row IDs might change if sheet is restructured
- **Mitigation**: Implement fallback to email+name combination matching

### Edge Case 2: Missing Row IDs
- **Risk**: Some rows might not have row ID in column 0
- **Mitigation**: Skip row_id for those entries, use legacy link-based detection

### Edge Case 3: Data Migration Issues
- **Risk**: Existing CSV data might be corrupted during schema update
- **Mitigation**: Atomic operations, comprehensive backups, rollback plan

### Edge Case 4: Concurrent Access
- **Risk**: Multiple scraping processes running simultaneously
- **Mitigation**: Existing file locking mechanisms should prevent this

## Success Metrics

1. **Correctness**: All 218 people with links captured (currently 211)
2. **Efficiency**: CSV size reduced from 7,893 to ~471 rows
3. **Reliability**: No duplicates in subsequent runs
4. **Performance**: Scraping time remains under 60 seconds

## Rollback Plan

1. **Immediate Rollback**: Restore backup CSV file
2. **Code Rollback**: Revert scraping logic to previous version
3. **Data Recovery**: Re-run pipeline with old logic if needed

## Current Status

- **Branch**: `fix-google-sheet-scraping`
- **Missing People**: 7 (rows 7-14 in Google Sheet)
- **Current CSV Rows**: 7,893 (should be ~471)
- **People with Links**: 211/218 captured

This plan provides a comprehensive, low-risk approach to fixing the Google Sheet scraping issues while maintaining data integrity and system reliability.