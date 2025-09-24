import pymysql
import os

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASS", "aa3357")
DB_NAME = os.getenv("DB_NAME", "sakila")
DB_PORT = int(os.getenv("DB_PORT", 3306))

def get_conn():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME,
        port=DB_PORT,
        cursorclass=pymysql.cursors.DictCursor
    )
