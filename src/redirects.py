import os
import pymysql.cursors
from dotenv import load_dotenv

import utils as u

def _connect_to_enwiki_db():
  load_dotenv(dotenv_path='.env')
  DATABASE_NAME = "enwiki"
  DATABASE_USER = os.getenv("DBUSER")
  DATABASE_PASSWORD = os.getenv("DBPASS")
  DATABASE_HOST = os.getenv("DBHOST")
  connection = pymysql.connect(host=DATABASE_HOST,
                               user=DATABASE_USER,
                               password=DATABASE_PASSWORD,
                               db=DATABASE_NAME,
                               charset='utf8mb4',
                               use_unicode=True,
                               cursorclass=pymysql.cursors.DictCursor)
  return connection

def _fetch_redirects_rows(cursor):
  cursor.execute("select page.page_title as redirect_from, redirect.rd_title as redirect_to from page inner join redirect on redirect.rd_from = page.page_id")
  return u.build_cursor_generator(cursor)

def _build_redirects_lookup(redirects_rows):
  lookup = {}
  for row in redirects_rows:
    from_page = row['redirect_from'].replace(b'_', b' ').decode('utf-8')
    to_page = row['redirect_to'].replace(b'_', b' ').decode('utf-8')
    lookup[from_page] = to_page
  return lookup

def get_redirects_lookup():
  connection = _connect_to_enwiki_db()
  try:
    with connection.cursor() as cursor:
      cursor.execute("SET NAMES utf8mb4;")
      cursor.execute("SET CHARACTER SET utf8mb4;")
      cursor.execute("SET character_set_connection=utf8mb4;")
      redirects_rows = _fetch_redirects_rows(cursor)
      redirects_lookup = _build_redirects_lookup(redirects_rows)
  finally:
    connection.close()
  return redirects_lookup
