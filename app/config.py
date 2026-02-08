import os

# --- Database Hosts ---
POSTGRES_HOST = "postgres"
MONGO_HOST = "mongo"
REDIS_HOST = "redis"

# --- Data Loading Settings ---
DATA_FILE_PATH = os.path.join("data", "arxiv-metadata-oai-snapshot.json")
RECORD_LIMIT = 1000000

# --- Benchmark Settings ---
BENCHMARK_N = 500
BENCHMARK_BULK_N = 500
RESULTS_CSV_PATH = "benchmark_results.csv"

# --- Sample Data for Tests ---
SAMPLE_PAPER_ID = '0704.0005'
SAMPLE_CATEGORY = 'hep-th'
SAMPLE_PAPER_ORIGINAL_SUBMITTER = 'Pavel Exner'

RARE_WORD_SEARCH_TERM = 'galaxy'