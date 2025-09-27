# CORE WORKFLOW - NEVER FORGET THIS IS ALL WE'RE TRYING TO DO

## The Simple 6-Step Process:

1. **Download a local copy** of the Google Sheet:
   `https://docs.google.com/spreadsheets/u/1/d/e/2PACX-1vRqqjqoaj8sEZBfZRw0Og7g8ms_0yTL2MsegTubcjhhBnXr1s1jFBwIVAsbkyj1xD0TMj06LvGTQIHU/pubhtml?pli=1#`

2. **Extract the Google Doc link** from the "name" column (if the link exists)

3. **Scrape the contents** of that Google Doc (if we have access to it)

4. **Extract links** from the scraped contents (if they exist)

5. **Download the links** (YouTube videos, Drive folders, Drive files)

6. **Correctly map** the downloaded content to the Name/Email/Type columns so that the data isn't lost

## CRITICAL PRINCIPLE:
This is a SIMPLE workflow. We overcomplicated it. Strip everything down to JUST these core minimal parts. Add complexity back only as absolutely necessary.

## Usage - Unified Script (simple_workflow.py):

### Basic Mode (Fast - Just 5 columns):
```bash
python3 simple_workflow.py --basic
```
Extracts: `row_id, name, email, type, link` for all 496 people (~2 seconds)

### Text Extraction Mode (Basic + Document Text):
```bash
python3 simple_workflow.py --text
```
Extracts: `row_id, name, email, type, link, document_text, processed, extraction_date` with batch processing (~30-60 minutes)

**Text Mode Features:**
- **Batch processing**: Process documents in configurable batches (default: 10)
- **Resumable**: Continue from where you left off if interrupted
- **Retry logic**: Automatic retry with configurable attempts
- **Progress tracking**: Saves progress and failed extractions
- **Error handling**: Marks failed extractions clearly

### Full Mode (Complete processing):
```bash
python3 simple_workflow.py
```
Extracts all data with document processing and link extraction (~1+ hours)

### Text Extraction Options:
```bash
# Test with limited documents
python3 simple_workflow.py --text --test-limit 5

# Custom batch size (larger batches = faster, but less resumable granularity)
python3 simple_workflow.py --text --batch-size 20

# Resume interrupted extraction
python3 simple_workflow.py --text --resume

# Retry previously failed extractions
python3 simple_workflow.py --text --retry-failed

# Custom output file
python3 simple_workflow.py --text --output my_text_data.csv
```

### Testing with limited records:
```bash
python3 simple_workflow.py --basic --test-limit 10
python3 simple_workflow.py --test-limit 5
```

## Success Criteria:
- Name/Email/Type data is preserved and correctly mapped to downloaded content
- No data loss during the process
- Clean, minimal implementation focusing only on these 6 steps
- DRY principle applied - single script handles both basic and full processing