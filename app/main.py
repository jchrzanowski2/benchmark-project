from . import db_connections
from . import data_loader
from . import benchmarks
from . import results_handler
import argparse

def main():

    parser = argparse.ArgumentParser(description="Benchmark CRUD operations for different databases.")
    parser.add_argument('--action', type=str, required=True, choices=['load', 'benchmark', 'run_all'],
                        help="Action to perform: 'load' (loads data), 'benchmark' (runs tests on existing data), 'run_all' (does both).")
    args = parser.parse_args()

    pg_conn = db_connections.connect_to_postgres()
    mongo_db = db_connections.connect_to_mongo()
    redis_conn = db_connections.connect_to_redis()

    if pg_conn is None or mongo_db is None or redis_conn is None:
        print("\nCould not establish connection with all databases. Aborting.")
        return

    try:
        if args.action in ['load', 'run_all']:
            print("\n--- ACTION: LOADING DATA ---")
            data_loader.load_data_to_postgres(pg_conn)
            data_loader.load_data_to_mongo(mongo_db)
            data_loader.load_data_to_redis(redis_conn)
            print("\n--- DATA LOADING COMPLETE ---")

        if args.action in ['benchmark', 'run_all']:
            print("\n--- ACTION: RUNNING BENCHMARKS ---")
            all_results = []
            all_results.extend(benchmarks.benchmark_postgres(pg_conn))
            all_results.extend(benchmarks.benchmark_mongo(mongo_db))
            all_results.extend(benchmarks.benchmark_redis(redis_conn))
            
            results_handler.save_results_to_csv(all_results)
            
            print("\n--- BENCHMARKING COMPLETE, CLEANING UP... ---")
            benchmarks.cleanup_postgres(pg_conn)
            benchmarks.cleanup_mongo(mongo_db)
            benchmarks.cleanup_redis(redis_conn)
            print("--- CLEANUP COMPLETE ---")

    finally:
        print("\nClosing database connections...")
        if pg_conn is not None: pg_conn.close()
        if mongo_db is not None: mongo_db.client.close()
        if redis_conn is not None: redis_conn.close()
        
    print(f"\n--- Action '{args.action}' finished successfully ---")

if __name__ == "__main__":
    main()