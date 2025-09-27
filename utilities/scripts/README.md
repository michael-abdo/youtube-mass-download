# Auxiliary Scripts

This directory contains auxiliary scripts that extend or modify the core pipeline functionality.

## Scripts in this directory:

- `run_extract_links.py` - Standalone runner for link extraction
- `update_drive_links.py` - Updates Google Drive links in the CSV file
- `update_drive_links_all.py` - Batch update of all Google Drive links
- `update_extract_results.py` - Updates extraction results in the CSV file

These scripts are NOT part of the core pipeline workflow and are used for:
- Post-processing data
- Fixing specific issues
- Running individual components standalone
- Batch updates and corrections

For the main pipeline, use `run_complete_workflow.py` in the root directory.