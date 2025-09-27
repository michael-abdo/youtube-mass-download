# Placeholder Migration Documentation

## Overview

This document describes the migration from dash (`-`) placeholders to proper null values (`None`/`NaN`) in the CSV data pipeline.

## Background: The Problem

### Root Cause
The system was using `-` as a placeholder for empty YouTube playlist and Google Drive URLs. However, this created a conflict with CSV injection protection:

1. **Extraction**: When no YouTube/Drive links found → insert `-` as placeholder
2. **CSV Write**: Sanitization sees `-` as potential formula injection → prefixes with quote
3. **Result**: `-` becomes `'-` in the CSV file
4. **Download**: Reads `'-`, strips quote, tries to download `-` → fails with "Invalid URL"

### Evidence
- 61 YouTube download failures with error: "Invalid YouTube URL: URL must include protocol and domain: -"
- CSV contained mix of `-` and `'-` values
- Downloads were failing due to data transformation, not actual URL issues

## The Solution

### Design Decision
Replace all placeholder values with proper null representation (`None` in Python, `NaN` in pandas).

### Benefits
1. **No CSV injection conflict**: None/NaN values don't trigger quote prefixing
2. **Cleaner data model**: Proper null handling instead of magic strings
3. **Better validation**: Can use pandas null checks (`pd.isna()`)
4. **Consistent with existing patterns**: System already has `safe_get_na_value()`

## Migration Process

### 1. Code Updates

#### Updated Functions
- `process_url()` - Returns None instead of `-` for empty values
- `download_youtube_with_context()` - Checks for null URLs before processing
- `download_drive_with_context()` - Checks for null URLs before processing
- `run_complete_workflow.py` - Uses `pd.notna()` instead of string comparison
- Alternative workflow scripts - Updated to handle None values

#### Removed Features
- `use_dash_for_empty` parameter from `process_url()`
- String comparisons for `-` placeholder

### 2. Data Migration

#### Migration Script: `scripts/migrate_placeholders.py`

**Features:**
- Converts both `-` and `'-` to NaN
- Handles youtube_playlist and google_drive columns
- Creates automatic backup before migration
- Dry-run mode for preview
- Validates CSV integrity before and after

**Usage:**
```bash
# Preview changes (dry run)
python scripts/migrate_placeholders.py

# Apply migration
python scripts/migrate_placeholders.py --apply
```

### 3. Migration Results

**Before Migration:**
- YouTube: 1 dash (`-`), 176 quoted dash (`'-`)
- Drive: 1 dash (`-`), 193 quoted dash (`'-`)
- Total affected rows: 224

**After Migration:**
- All placeholder values converted to NaN
- CSV validation passes
- Downloads skip null values gracefully

## Validation

### Testing Performed
1. **Unit Testing**: Verified `process_url()` returns None for empty values
2. **Integration Testing**: Ran workflow with migrated data
3. **Error Handling**: Confirmed null URLs are skipped with clear messages
4. **New Extractions**: Verified new extractions create None, not `-`

### Results
- ✅ No more "Invalid URL: -" errors
- ✅ Null values handled gracefully throughout pipeline
- ✅ CSV validation passes with consistent null representation
- ✅ Downloads fail only for legitimate reasons (404, private content)

## Best Practices Going Forward

### DO:
- Use `None` for missing values in Python code
- Use `pd.isna()` or `pd.notna()` for null checks
- Let pandas handle None → NaN conversion automatically

### DON'T:
- Use string placeholders like `-`, `N/A`, `null`
- Compare with string literals for null checks
- Mix null representations within same column

## Rollback Procedure

If issues arise after migration:

1. **Restore from backup:**
   ```bash
   cp outputs/output.csv.backup_migrate_placeholders_[timestamp] outputs/output.csv
   ```

2. **Revert code changes:**
   ```bash
   git checkout [commit-before-migration]
   ```

3. **Rerun with old behavior** (temporary workaround)

## Future Improvements

1. **Consider separate boolean columns**: `has_youtube_content`, `has_drive_content`
2. **Implement proper null handling in CSV writer**: Avoid string conversion
3. **Add data quality checks**: Validate URLs before storage
4. **Enhance error messages**: Distinguish "no URL" from "invalid URL"

## References

- Root Cause Analysis: `ROOT_CAUSE_ANALYSIS.md`
- CSV Null Handling Convention: `CLAUDE.md` 
- Migration Script: `scripts/migrate_placeholders.py`
- Original Issue: CSV injection protection transforming placeholders