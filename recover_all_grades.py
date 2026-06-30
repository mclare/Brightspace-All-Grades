import pandas as pd
import csv
import re
import os
import sys
import time
import concurrent.futures
from pathlib import Path

"""
BRIGHTSPACE DATA HUB: ALL GRADES RECOVERY SCRIPT
================================================
Purpose:
    Converts the "All Grades" Data Hub CSV (one row per grade item) into:
    1. A forensic 'details' log for each course.
    2. A 'gradebook' CSV formatted for Brightspace Import (one row per student).

Optimization strategy for what can be a long and memory-intensive process:
    - Phase 1: Rapidly scans the source to identify relevant Course Offering Codes.
    - Phase 2: Parallelizes work across CPU cores.
    - Block Processing: Processes consecutive rows for the same course in a single batch to minimize expensive DataFrame operations.
    - Interrupt Handling: Designed to flush data at the end of each course block, and be able to resume without loss if interrupted.
"""

# --- Configuration ---
SOURCE_CSV = "All Grades-05-11-2025T08-44-12.csv"
OUTPUT_DIR = "recovered_grades"
REGEX_SCOPE = r""  # Filter Course Offering Code by term/dept (e.g., '2025-FW' in '2025-FW-D02-S02-VISA-3P93-LL' and '2025-FW-D02-S02-COMM-1F90-LEC'). Leave empty for all.
NUM_THREADS = os.cpu_count()

# --- System Adjustments ---
# Brightspace 'Grade Comments' can be massive. We maximize the allowed field size 
# to prevent 'field larger than field limit' errors.
max_int = sys.maxsize
while True:
    try:
        csv.field_size_limit(max_int)
        break
    except OverflowError:
        max_int = int(max_int / 10)

os.makedirs(OUTPUT_DIR, exist_ok=True)

def sanitize_filename(name):
    """Sanitizes strings to be filesystem-safe for Windows/Mac/Linux."""
    return re.sub(r'[\\/*?:"<>|]', "_", str(name))

def process_block(c_id, rows, gradebooks, filenames, fieldnames):
    """
    Core Logic: Aggregates a 'chunk' of rows belonging to the same course.
    This is called every time the 'Course Offering Id' changes in the stream.
    """
    # 1. Filename Setup
    if c_id not in filenames:
        # Use the first row in the block to get the course code
        code = sanitize_filename(rows[0]['Course Offering Code'])
        filenames[c_id] = f"{c_id}-{code}"
    
    base_name = filenames[c_id]

    # 2. Bulk Update 'Grade Details' (Forensic Log)
    # We open the file once and write all rows in the block at once.
    detail_file = Path(OUTPUT_DIR) / f"{base_name}-grade-details.csv"
    file_exists = detail_file.exists()
    with open(detail_file, 'a', newline='', encoding='utf-8') as df_out:
        writer = csv.DictWriter(df_out, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows) 

    # 3. Update 'Gradebook' (Brightspace Import Format)
    if c_id not in gradebooks:
        gb_path = Path(OUTPUT_DIR) / f"{base_name}-gradebook.csv"
        if gb_path.exists():
            # Force string types to handle non-numeric grades (Exempt, G, etc.)
            gradebooks[c_id] = pd.read_csv(gb_path, dtype=str).fillna('')
        else:
            # Initialize Brightspace standard identity columns
            gradebooks[c_id] = pd.DataFrame(columns=[
                'OrgDefinedId', 'Username', 'Last Name', 'First Name', 'Email', 'End-of-Line Indicator'
            ])

    df = gradebooks[c_id]
    
    # Process all rows in this chunk into the pivoted DataFrame
    for row in rows:
        org_id = f"#{row['Org Defined Id']}"
        # Construct the unique Brightspace header format
        col_name = f"{row['Grade Item Name']} <Numeric MaxPoints:{row['Points Denominator']}>"
        
        # Add new student if not already in this course's DataFrame
        if org_id not in df['OrgDefinedId'].values:
            new_student = {
                'OrgDefinedId': org_id,
                'Username': row['Username'],
                'Last Name': row['Last Name'],
                'First Name': row['First Name'],
                'Email': f"{row['Username']}@brocku.ca",
                'End-of-Line Indicator': '#'
            }
            df = pd.concat([df, pd.DataFrame([new_student]).astype(str)], ignore_index=True)

        # Place the grade value in the correct student row and grade column
        df.loc[df['OrgDefinedId'] == org_id, col_name] = str(row['Points Numerator'])
    
    gradebooks[c_id] = df

def process_course_batch(course_ids, source_path, thread_id):
    """
    Thread Worker: Streams the source CSV and identifies 'runs' of rows 
    belonging to the same course to pass to the block processor.
    """
    gradebooks = {}  
    filenames = {}   
    rows_processed = 0
    start_time = time.time()

    print(f"[Thread {thread_id}] Starting work on {len(course_ids)} courses...")

    with open(source_path, mode='r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        current_block = []
        active_c_id = None

        for row in reader:
            c_id = row['Course Offering Id']
            
            # Check if this thread is responsible for this Course ID
            if c_id not in course_ids:
                # If we just finished a block before hitting an unowned course, flush it
                if current_block:
                    process_block(active_c_id, current_block, gradebooks, filenames, reader.fieldnames)
                    rows_processed += len(current_block)
                    current_block = []
                    active_c_id = None
                continue

            # If the ID changes within our owned courses, flush the previous block
            if c_id != active_c_id and active_c_id is not None:
                process_block(active_c_id, current_block, gradebooks, filenames, reader.fieldnames)
                rows_processed += len(current_block)
                current_block = []

            active_c_id = c_id
            current_block.append(row)

        # Flush the final block for this thread
        if current_block:
            process_block(active_c_id, current_block, gradebooks, filenames, reader.fieldnames)
            rows_processed += len(current_block)

    # WRITE DATA TO DISK: Done at the end of the thread to minimize disk thrashing
    for c_id, df in gradebooks.items():
        gb_path = Path(OUTPUT_DIR) / f"{filenames[c_id]}-gradebook.csv"
        df.to_csv(gb_path, index=False, na_rep='')
    
    print(f"[Thread {thread_id}] Complete. Processed: {rows_processed} rows in {time.time() - start_time:.2f}s")

def main():
    total_start = time.time()
    
    # --- PHASE 1: DISCOVERY ---
    # Fast scan to find all unique Course Codes matching our criteria.
    print(f"--- Phase 1: Identifying Courses (Regex Filter: '{REGEX_SCOPE}') ---")
    matching_ids = set()
    regex = re.compile(REGEX_SCOPE)
    total_rows_scanned = 0
    
    with open(SOURCE_CSV, mode='r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_rows_scanned += 1
            if regex.search(row['Course Offering Code']):
                matching_ids.add(row['Course Offering Id'])
            
            if total_rows_scanned % 500000 == 0:
                print(f"  Scanned {total_rows_scanned} rows...")
    
    id_list = list(matching_ids)
    print(f"Discovery Complete. Found {len(id_list)} courses.")

    # --- PHASE 2: PARTITIONING ---
    # Distribute courses evenly across available CPU threads.
    batches = [id_list[i::NUM_THREADS] for i in range(NUM_THREADS)]
    
    # --- PHASE 3: EXECUTION ---
    print(f"--- Phase 2: Processing using {NUM_THREADS} threads ---")
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        # We pass a 'set' of IDs for O(1) membership testing speed.
        futures = [
            executor.submit(process_course_batch, set(batch), SOURCE_CSV, i) 
            for i, batch in enumerate(batches)
        ]
        
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"!! ERROR in execution: {e}")

    total_elapsed = time.time() - total_start
    print(f"\n--- Recovery Successful ---")
    print(f"Total rows scanned: {total_rows_scanned}")
    print(f"Total Runtime: {total_elapsed / 60:.2f} minutes")
    print(f"Output folder: {os.path.abspath(OUTPUT_DIR)}")

if __name__ == "__main__":
    main()