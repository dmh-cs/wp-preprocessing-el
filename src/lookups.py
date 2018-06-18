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

def _fetch_page_titles(cursor):
  cursor.execute('select page_title from page where page_namespace = 0 and page_is_redirect = 0')
  return u.build_cursor_generator(cursor)

def _fetch_redirects_rows(cursor):
  cursor.execute("select page.page_title as redirect_from, redirect.rd_title as redirect_to from page inner join redirect on redirect.rd_from = page.page_id where page.page_namespace = 0")
  return u.build_cursor_generator(cursor)

def _build_redirects_lookup(redirects_rows):
  lookup = {}
  for row in redirects_rows:
    from_page = row['redirect_from'].replace('_', ' ')
    to_page = row['redirect_to'].replace('_', ' ')
    lookup[from_page] = to_page
    if from_page.lower() not in lookup:
      lookup[from_page.lower()] = to_page
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

def get_page_title_lookup_and_nonunique_page_titles():
  connection = _connect_to_enwiki_db()
  try:
    with connection.cursor() as cursor:
      cursor.execute("SET NAMES utf8mb4;")
      cursor.execute("SET CHARACTER SET utf8mb4;")
      cursor.execute("SET character_set_connection=utf8mb4;")
      pages = _fetch_page_titles(cursor)
      page_title_lookup = {}
      nonunique_page_titles = set()
      for page in pages:
        lower_cleaned_title = page['page_title'].replace('_', ' ').lower()
        if lower_cleaned_title in page_title_lookup:
          nonunique_page_titles.add(lower_cleaned_title)
        else:
          page_title_lookup[lower_cleaned_title] = page['page_title']
      for lower_cleaned_title in nonunique_page_titles:
        page_title_lookup.pop(lower_cleaned_title)
  finally:
    connection.close()
  return page_title_lookup, nonunique_page_titles
