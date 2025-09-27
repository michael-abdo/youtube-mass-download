#!/bin/bash

# Navigate to the proposals directory
PROPOSALS_DIR="/Users/Mike/Desktop/programming/2_proposals/upwork"
SUBMIT_DIR="/Users/Mike/Desktop/programming/1_proposal_automation/3_submit_proposal"

# Check if directories exist
if [ ! -d "$PROPOSALS_DIR" ]; then
    echo "❌ Proposals directory not found: $PROPOSALS_DIR"
    exit 1
fi

if [ ! -d "$SUBMIT_DIR" ]; then
    echo "❌ Submit directory not found: $SUBMIT_DIR"
    exit 1
fi

echo "🔍 Finding all valid job IDs in: $PROPOSALS_DIR"
echo "================================================"

# Find all directories that match the job ID pattern (18-digit numbers starting with 02)
# and store them in an array
job_ids=()
while IFS= read -r dir; do
    basename_dir=$(basename "$dir")
    # Check if it matches the pattern: 18 digits starting with 02
    if [[ "$basename_dir" =~ ^02[0-9]{16}$ ]]; then
        job_ids+=("$basename_dir")
    fi
done < <(find "$PROPOSALS_DIR" -maxdepth 1 -type d)

# Sort the job IDs
IFS=$'\n' sorted_job_ids=($(sort <<<"${job_ids[*]}"))
unset IFS

# Display found job IDs
echo "📋 Found ${#sorted_job_ids[@]} valid job IDs:"
for id in "${sorted_job_ids[@]}"; do
    echo "   - $id"
done
echo "================================================"

# Ask for confirmation
read -p "🤔 Do you want to submit all ${#sorted_job_ids[@]} jobs? (y/n): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ Cancelled"
    exit 0
fi

# Change to submit directory
cd "$SUBMIT_DIR" || exit 1

# Track statistics
successful=0
failed=0
unavailable=0

echo ""
echo "🚀 Starting bulk submission..."
echo "================================================"

# Submit each job
for i in "${!sorted_job_ids[@]}"; do
    job_id="${sorted_job_ids[$i]}"
    echo ""
    echo "📊 Progress: $((i+1))/${#sorted_job_ids[@]}"
    echo "🎯 Submitting job: $job_id"
    echo "---"
    
    # Run the submission and capture output
    if output=$(node utilities/iterative_file_upload_agent.js "$job_id" 2>&1); then
        # Check if job was unavailable
        if echo "$output" | grep -q "Job is no longer available"; then
            echo "⚠️  Job no longer available (auto-updated in sheets)"
            ((unavailable++))
        else
            echo "✅ Successfully submitted!"
            ((successful++))
        fi
    else
        echo "❌ Failed to submit"
        ((failed++))
    fi
    
    # Optional: Add a small delay between submissions to avoid rate limiting
    if [ $((i+1)) -lt ${#sorted_job_ids[@]} ]; then
        echo "⏳ Waiting 3 seconds before next submission..."
        sleep 3
    fi
done

# Final summary
echo ""
echo "================================================"
echo "📈 FINAL SUMMARY"
echo "================================================"
echo "✅ Successful submissions: $successful"
echo "⚠️  Jobs no longer available: $unavailable"
echo "❌ Failed submissions: $failed"
echo "📊 Total processed: ${#sorted_job_ids[@]}"
echo "================================================"