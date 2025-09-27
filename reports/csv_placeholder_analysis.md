# CSV Placeholder and Injection Protection Analysis

## Summary

The codebase uses '-' as a placeholder value for empty YouTube playlist and Google Drive links. However, the CSV injection protection in `sanitize_csv_value()` function prefixes any value starting with '-' with a single quote, turning '-' into "'-".

## Current Placeholder Usage

### 1. Where '-' is Used
- **extract_links.py**: Returns '-' for empty YouTube playlists and Google Drive links when `use_dash_for_empty=True`
- **CSV files**: The output.csv contains '-' values in youtube_playlist and google_drive columns
- **Workflow scripts**: Check for '-' values to determine if links are empty

### 2. Code References
```python
# extract_links.py (lines 668-690)
if use_dash_for_empty:
    if not yt_playlist_url:
        yt_playlist_url = "-"
    if not drive_links:
        drive_links = ["-"]
```

```python
# run_complete_workflow.py (lines 143, 209)
if drive_url and str(drive_url).strip() not in ['', '-', 'nan']:
if youtube_url and str(youtube_url).strip() not in ['', '-', 'nan']:
```

```python
# utilities/alternative_workflows/download_all_media.py
drive_links = [link for link in drive_links if link and link != '-']
youtube_links = [link for link in youtube_links if link and link != '-']
```

## CSV Injection Protection

The `sanitize_csv_value()` function in `utils/validation.py` protects against CSV injection by prefixing values that start with special characters:

```python
# validation.py (lines 313-317)
# Prevent formula injection by prefixing with single quote
# if value starts with =, +, -, @, tab, carriage return
if value and value[0] in ('=', '+', '-', '@', '\t', '\r'):
    value = "'" + value
```

This means:
- Input: `-`
- Output: `'-`

## Impact Analysis

### 1. Breaking Changes
When sanitize_csv_value() is applied to '-' placeholders:
- The value changes from `-` to `'-`
- Scripts checking for `== '-'` will fail to match `'-`
- This could break downstream processing that expects exact '-' values

### 2. Affected Components
- **run_complete_workflow.py**: Checks for '-' when determining if links should be processed
- **Alternative workflows**: Multiple scripts filter out '-' values
- **cleanup_malformed_urls.py**: Returns '-' for invalid URLs

### 3. Current State
The system appears to be working because:
- The CSV writing may not be using sanitize_csv_value() for all fields
- Or the '-' values are being written directly without sanitization
- The actual CSV shows '-' not "'-", suggesting sanitization isn't applied

## Recommendations

### Option 1: Change Placeholder (Recommended)
Use a different placeholder that doesn't trigger injection protection:
- `null` or `none` (descriptive)
- `empty` (clear meaning)
- `N/A` (standard notation)
- Empty string with proper null handling

### Option 2: Exclude '-' from Protection
Modify sanitize_csv_value() to not prefix single '-':
```python
if value and value[0] in ('=', '+', '-', '@', '\t', '\r'):
    # Don't prefix single dash placeholder
    if value != '-':
        value = "'" + value
```

### Option 3: Update All Checks
Update all code that checks for '-' to also check for "'-":
```python
if str(value).strip() not in ['', '-', "'-", 'nan']:
```

### Option 4: Use Proper Null Handling
Follow the codebase's null handling convention:
- Use `None` which becomes `NaN` in pandas
- This aligns with safe_get_na_value() function
- Prevents false positive corruption detection

## Conclusion

The current system works but has a latent issue where applying CSV injection protection would break placeholder detection. The best solution is to:

1. Replace '-' with proper null values (None/NaN)
2. Update all checks to use pandas null detection: `pd.isna(value)`
3. This aligns with the codebase's existing null handling strategy
4. Prevents both injection vulnerabilities and placeholder detection issues