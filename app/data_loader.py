import time
import json
from datetime import datetime
from . import config

def get_or_create_id(cursor, table, id_column, value_column, value):
    cursor.execute(f"SELECT {id_column} FROM {table} WHERE {value_column} = %s", (value,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        cursor.execute(f"INSERT INTO {table} ({value_column}) VALUES (%s) RETURNING {id_column}", (value,))
        return cursor.fetchone()[0]

def load_data_to_postgres(conn):
    print("\n--- Starting data load to PostgreSQL ---")
    start_time = time.time()
    with conn.cursor() as cur:
        print("  Cleaning PostgreSQL tables...")
        cur.execute("TRUNCATE TABLE papers, authors, categories RESTART IDENTITY CASCADE;")
        conn.commit()
        with open(config.DATA_FILE_PATH, 'r') as f:
            for i, line in enumerate(f):
                if config.RECORD_LIMIT and i >= config.RECORD_LIMIT: break
                record = json.loads(line)
                cur.execute("INSERT INTO papers (id, title, abstract, doi, submitter, update_date) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING",
                            (record.get('id'), record.get('title'), record.get('abstract'), record.get('doi'), record.get('submitter'), record.get('update_date')))
                authors = record.get('authors_parsed', [])
                for author_parts in authors:
                    author_name = " ".join(filter(None, [author_parts[1], author_parts[0]]))
                    if author_name:
                        author_id = get_or_create_id(cur, 'authors', 'author_id', 'author_name', author_name)
                        cur.execute("INSERT INTO paper_authors (paper_id, author_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (record['id'], author_id))
                categories = record.get('categories', '').split()
                for category_name in categories:
                    category_id = get_or_create_id(cur, 'categories', 'category_id', 'category_name', category_name)
                    cur.execute("INSERT INTO paper_categories (paper_id, category_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (record['id'], category_id))
                if (i + 1) % 5000 == 0: print(f"  Processed {i+1} records...")
    conn.commit()
    print(f"✅ Finished loading data to PostgreSQL. Time: {time.time() - start_time:.2f} s")


def load_data_to_mongo(db):
    print("\n--- Starting data load to MongoDB ---")
    start_time = time.time()
    print("  Cleaning 'papers' collection in MongoDB...")
    db['papers'].drop()
    papers_collection = db['papers']
    batch = []
    with open(config.DATA_FILE_PATH, 'r') as f:
        for i, line in enumerate(f):
            if config.RECORD_LIMIT and i >= config.RECORD_LIMIT: break
            record = json.loads(line)
            record['_id'] = record['id']
            try: record['update_date'] = datetime.strptime(record['update_date'], '%Y-%m-%d')
            except (ValueError, TypeError): record['update_date'] = None
            batch.append(record)
            if len(batch) >= 1000:
                papers_collection.insert_many(batch)
                batch = []
                if (i + 1) % 5000 == 0: print(f"  Processed {i+1} records...")
        if batch: papers_collection.insert_many(batch)
    print(f"✅ Finished loading data to MongoDB. Time: {time.time() - start_time:.2f} s")

def load_data_to_redis(r):
    print("\n--- Starting data load to Redis ---")
    start_time = time.time()
    print("  Cleaning Redis database...")
    r.flushall()
    pipe = r.pipeline()
    with open(config.DATA_FILE_PATH, 'r') as f:
        for i, line in enumerate(f):
            if config.RECORD_LIMIT and i >= config.RECORD_LIMIT: break
            record = json.loads(line)
            paper_id = record['id']
            paper_data = {'title': record.get('title', ''), 'abstract': record.get('abstract', ''), 'update_date': record.get('update_date', '')}
            pipe.hset(f"paper:{paper_id}", mapping=paper_data)
            categories = record.get('categories', '').split()
            for cat in categories: pipe.sadd(f"category:{cat}", paper_id)
            authors = record.get('authors_parsed', [])
            for author_parts in authors:
                author_name = "_".join(filter(None, [author_parts[1], author_parts[0]])).replace(" ", "_")
                if author_name: pipe.sadd(f"author:{author_name}", paper_id)
            if (i + 1) % 1000 == 0:
                pipe.execute()
                if (i + 1) % 5000 == 0: print(f"  Processed {i+1} records...")
        pipe.execute()
    print(f"✅ Finished loading data to Redis. Time: {time.time() - start_time:.2f} s")