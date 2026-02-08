import psycopg2
import pymongo
import redis
from . import config

def connect_to_postgres():
    try:
        conn = psycopg2.connect(host=config.POSTGRES_HOST, dbname="arxiv_db", user="user", password="password")
        print("✅ Connected to PostgreSQL!")
        return conn
    except psycopg2.OperationalError as e:
        print(f"❌ Connection to PostgreSQL failed: {e}")
        return None

def connect_to_mongo():
    try:
        client = pymongo.MongoClient(host=config.MONGO_HOST, port=27017, serverSelectionTimeoutMS=5000)
        client.server_info()
        print("✅ Connected to MongoDB!")
        return client['arxiv_db']
    except pymongo.errors.ConnectionFailure as e:
        print(f"❌ Connection to MongoDB failed: {e}")
        return None

def connect_to_redis():
    try:
        r = redis.Redis(host=config.REDIS_HOST, port=6379, decode_responses=True)
        r.ping()
        print("✅ Connected to Redis!")
        return r
    except redis.exceptions.ConnectionError as e:
        print(f"❌ Connection to Redis failed: {e}")
        return None