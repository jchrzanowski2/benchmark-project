import time
import uuid
from datetime import datetime
from . import config

# --- BENCHMARK FUNCTIONS ---

def benchmark_postgres(conn):
    print("\n--- Running BENCHMARK: PostgreSQL ---")
    results = []
    
    with conn.cursor() as cur:
        # --- WRITE OPERATIONS ---
        ids_to_delete = [f"bench-{uuid.uuid4()}" for _ in range(config.BENCHMARK_BULK_N)]
        start = time.time()
        for paper_id in ids_to_delete:
            cur.execute("INSERT INTO papers (id, title) VALUES (%s, %s)", (paper_id, 'Bulk Create Test'))
        conn.commit()
        results.append({'database': 'PostgreSQL', 'operation': 'WRITE_CREATE_BULK', 'time_seconds': time.time() - start, 'records_processed': config.BENCHMARK_BULK_N})

        # --- READ OPERATIONS ---
        start = time.time()
        for _ in range(config.BENCHMARK_N):
            cur.execute("SELECT * FROM papers WHERE id = %s", (config.SAMPLE_PAPER_ID,))
            cur.fetchone()
        results.append({'database': 'PostgreSQL', 'operation': 'READ_BY_PRIMARY_KEY', 'time_seconds': time.time() - start, 'records_processed': config.BENCHMARK_N})

        start = time.time()
        cur.execute("SELECT p.id, p.title FROM papers p JOIN paper_categories pc ON p.id = pc.paper_id JOIN categories c ON pc.category_id = c.category_id WHERE c.category_name = %s LIMIT 100", (config.SAMPLE_CATEGORY,))
        cur.fetchall()
        results.append({'database': 'PostgreSQL', 'operation': 'READ_JOIN_BY_CATEGORY', 'time_seconds': time.time() - start, 'records_processed': 100})
        
        start = time.time()
        cur.execute("SELECT id FROM papers WHERE abstract ILIKE %s LIMIT 100", (f'%{config.RARE_WORD_SEARCH_TERM}%',))
        cur.fetchall()
        results.append({'database': 'PostgreSQL', 'operation': 'READ_FULL_TEXT_SEARCH_NO_INDEX', 'time_seconds': time.time() - start, 'records_processed': 100})


        cur.execute("CREATE INDEX idx_papers_abstract_gin ON papers USING gin(to_tsvector('english', abstract));")
        conn.commit()
        start = time.time()
        cur.execute("SELECT id FROM papers WHERE to_tsvector('english', abstract) @@ to_tsquery('english', %s) LIMIT 100;", (config.RARE_WORD_SEARCH_TERM,))
        cur.fetchall()
        results.append({'database': 'PostgreSQL', 'operation': 'READ_FULL_TEXT_SEARCH_WITH_INDEX', 'time_seconds': time.time() - start, 'records_processed': 100})
        cur.execute("DROP INDEX idx_papers_abstract_gin;")
        conn.commit()

        start = time.time()
        cur.execute("SELECT COUNT(DISTINCT author_name) FROM authors")
        cur.fetchone()
        results.append({'database': 'PostgreSQL', 'operation': 'READ_AGGREGATE_COUNT', 'time_seconds': time.time() - start, 'records_processed': 1})

        # --- UPDATE & DELETE OPERATIONS ---
        start = time.time()
        for _ in range(config.BENCHMARK_N):
            cur.execute("UPDATE papers SET submitter = 'benchmark_runner' WHERE id = %s", (config.SAMPLE_PAPER_ID,))
        conn.commit()
        results.append({'database': 'PostgreSQL', 'operation': 'UPDATE_ONE_BY_ID', 'time_seconds': time.time() - start, 'records_processed': config.BENCHMARK_N})
        
        start = time.time()
        cur.execute("UPDATE papers SET doi = 'updated_by_benchmark' WHERE id LIKE 'bench-%'")
        conn.commit()
        results.append({'database': 'PostgreSQL', 'operation': 'UPDATE_MANY', 'time_seconds': time.time() - start, 'records_processed': config.BENCHMARK_BULK_N})

        start = time.time()
        for paper_id in ids_to_delete:
            cur.execute("DELETE FROM papers WHERE id = %s", (paper_id,))
        conn.commit()
        results.append({'database': 'PostgreSQL', 'operation': 'DELETE_BULK_BY_ID', 'time_seconds': time.time() - start, 'records_processed': len(ids_to_delete)})
    
    for res in results: res['timestamp'] = datetime.now().isoformat()
    return results

def benchmark_mongo(db):
    print("--- Running BENCHMARK: MongoDB ---")
    results = []
    papers = db.papers
    
    # --- WRITE OPERATIONS ---
    docs_to_insert = [{'_id': f"bench-{uuid.uuid4()}", 'title': 'Bulk Create Test'} for _ in range(config.BENCHMARK_BULK_N)]
    ids_to_delete = [doc['_id'] for doc in docs_to_insert]
    start = time.time()
    papers.insert_many(docs_to_insert)
    results.append({'database': 'MongoDB', 'operation': 'WRITE_CREATE_BULK', 'time_seconds': time.time() - start, 'records_processed': config.BENCHMARK_BULK_N})

    # --- READ OPERATIONS ---
    start = time.time()
    for _ in range(config.BENCHMARK_N):
        papers.find_one({'_id': config.SAMPLE_PAPER_ID})
    results.append({'database': 'MongoDB', 'operation': 'READ_BY_PRIMARY_KEY', 'time_seconds': time.time() - start, 'records_processed': config.BENCHMARK_N})
    
    start = time.time()
    list(papers.find({'categories': {'$regex': f'^{config.SAMPLE_CATEGORY}'}}).limit(100))
    results.append({'database': 'MongoDB', 'operation': 'READ_IN_ARRAY_BY_CATEGORY', 'time_seconds': time.time() - start, 'records_processed': 100})

    start = time.time()
    list(papers.find({'abstract': {'$regex': config.RARE_WORD_SEARCH_TERM, '$options': 'i'}}).limit(100))
    results.append({'database': 'MongoDB', 'operation': 'READ_FULL_TEXT_SEARCH_NO_INDEX', 'time_seconds': time.time() - start, 'records_processed': 100})
    
    start = time.time()
    list(papers.aggregate([
        {'$unwind': '$authors_parsed'},
        {'$group': {'_id': '$authors_parsed'}},
        {'$count': 'unique_authors'}
    ]))
    results.append({'database': 'MongoDB', 'operation': 'READ_AGGREGATE_COUNT', 'time_seconds': time.time() - start, 'records_processed': 1})
    
    papers.create_index([('abstract', 'text')])
    start = time.time()
    list(papers.find({'$text': {'$search': config.RARE_WORD_SEARCH_TERM}}).limit(100))
    results.append({'database': 'MongoDB', 'operation': 'READ_FULL_TEXT_SEARCH_WITH_INDEX', 'time_seconds': time.time() - start, 'records_processed': 100})
    papers.drop_index('abstract_text')

    # --- UPDATE OPERATIONS ---
    start = time.time()
    for _ in range(config.BENCHMARK_N):
        papers.update_one({'_id': config.SAMPLE_PAPER_ID}, {'$set': {'submitter': 'benchmark_runner'}})
    results.append({'database': 'MongoDB', 'operation': 'UPDATE_ONE_BY_ID', 'time_seconds': time.time() - start, 'records_processed': config.BENCHMARK_N})
    
    start = time.time()
    papers.update_many({'_id': {'$in': ids_to_delete}}, {'$set': {'doi': 'updated_by_benchmark'}})
    results.append({'database': 'MongoDB', 'operation': 'UPDATE_MANY', 'time_seconds': time.time() - start, 'records_processed': len(ids_to_delete)})

    # --- DELETE OPERATIONS ---
    start = time.time()
    papers.delete_many({'_id': {'$in': ids_to_delete}})
    results.append({'database': 'MongoDB', 'operation': 'DELETE_BULK_BY_ID', 'time_seconds': time.time() - start, 'records_processed': len(ids_to_delete)})

    for res in results: res['timestamp'] = datetime.now().isoformat()
    return results
def benchmark_redis(r):
    print("--- Running BENCHMARK: Redis ---")
    results = []
    
    # --- WRITE OPERATIONS ---
    ids_to_delete = [f"paper:bench-{uuid.uuid4()}" for _ in range(config.BENCHMARK_BULK_N)]
    start = time.time()
    pipe = r.pipeline()
    for paper_id in ids_to_delete:
        pipe.hset(paper_id, mapping={'title': 'Bulk Create Test'})
    pipe.execute()
    results.append({'database': 'Redis', 'operation': 'WRITE_CREATE_BULK', 'time_seconds': time.time() - start, 'records_processed': config.BENCHMARK_BULK_N})

    # --- READ OPERATIONS ---
    start = time.time()
    for _ in range(config.BENCHMARK_N):
        r.hgetall(f"paper:{config.SAMPLE_PAPER_ID}")
    results.append({'database': 'Redis', 'operation': 'READ_BY_PRIMARY_KEY', 'time_seconds': time.time() - start, 'records_processed': config.BENCHMARK_N})
    
    start = time.time()
    paper_ids_from_index = list(r.smembers(f"category:{config.SAMPLE_CATEGORY}"))[:100]
    pipe = r.pipeline()
    for paper_id in paper_ids_from_index:
        pipe.hgetall(f"paper:{paper_id}")
    pipe.execute()
    results.append({'database': 'Redis', 'operation': 'READ_VIA_SECONDARY_INDEX', 'time_seconds': time.time() - start, 'records_processed': len(paper_ids_from_index)})
    
    # --- UPDATE OPERATIONS ---
    start = time.time()
    for _ in range(config.BENCHMARK_N):
        r.hset(f"paper:{config.SAMPLE_PAPER_ID}", 'submitter', 'benchmark_runner')
    results.append({'database': 'Redis', 'operation': 'UPDATE_ONE_BY_ID', 'time_seconds': time.time() - start, 'records_processed': config.BENCHMARK_N})

    # --- DELETE OPERATIONS ---
    start = time.time()
    if ids_to_delete:
        r.delete(*ids_to_delete)
    results.append({'database': 'Redis', 'operation': 'DELETE_BULK_BY_ID', 'time_seconds': time.time() - start, 'records_processed': len(ids_to_delete)})

    for res in results: res['timestamp'] = datetime.now().isoformat()
    return results

def cleanup_postgres(conn):
    """Resets the database to its initial state after a benchmark run."""
    with conn.cursor() as cur:
        # 1. Restore the updated record to its original state
        cur.execute("UPDATE papers SET submitter = %s WHERE id = %s", (config.SAMPLE_PAPER_ORIGINAL_SUBMITTER, config.SAMPLE_PAPER_ID))
        # 2. Delete all records created during the benchmark
        cur.execute("DELETE FROM papers WHERE id LIKE 'bench-%'")
        conn.commit()
    print("  PostgreSQL cleanup finished.")

def cleanup_mongo(db):
    """Resets the database to its initial state after a benchmark run."""
    papers = db.papers
    # 1. Restore the updated record
    papers.update_one({'_id': config.SAMPLE_PAPER_ID}, {'$set': {'submitter': config.SAMPLE_PAPER_ORIGINAL_SUBMITTER}})
    # 2. Delete all records created during the benchmark
    papers.delete_many({'_id': {'$regex': '^bench-'}})
    print("  MongoDB cleanup finished.")

def cleanup_redis(r):
    """Resets the database to its initial state after a benchmark run."""
    # 1. Restore the updated record
    r.hset(f"paper:{config.SAMPLE_PAPER_ID}", 'submitter', config.SAMPLE_PAPER_ORIGINAL_SUBMITTER)
    # 2. Delete all keys created during the benchmark (requires scanning)
    for key in r.scan_iter("paper:bench-*"):
        r.delete(key)
    print("  Redis cleanup finished.")