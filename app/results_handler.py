import csv
import os
from datetime import datetime
from . import config

def save_results_to_csv(results):
    """Saves a list of result dictionaries to a CSV file."""
    file_exists = os.path.isfile(config.RESULTS_CSV_PATH)
    
    with open(config.RESULTS_CSV_PATH, mode='w', newline='') as csv_file:
        fieldnames = ['timestamp', 'database', 'operation', 'records_processed', 'time_seconds']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        
        if not file_exists:
            writer.writeheader()
            
        for result in results:
            writer.writerow(result)
            
    print(f"\n✅ Benchmark results appended to {config.RESULTS_CSV_PATH}")