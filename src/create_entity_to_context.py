import os
import pymysql.cursors
from dotenv import load_dotenv
from pathlib import Path
from progressbar import progressbar
from pymongo import MongoClient

from db import insert_wp_page, insert_category_associations, insert_link_contexts
from process_pages import process_seed_pages


def main():
  load_dotenv(dotenv_path=Path('../db') / '.env')
  DATABASE_NAME = os.getenv("DBNAME")
  DATABASE_USER = os.getenv("DBUSER")
  DATABASE_PASSWORD = os.getenv("DBPASS")
  DATABASE_HOST = os.getenv("DBHOST")

  client = MongoClient()
  dbname = 'enwiki'
  print('Reading from mongodb db', dbname)
  db = client[dbname]
  pages_db = db['pages']
  num_seed_pages = 10
  print('Fetching WP pages using', num_seed_pages, 'seed pages')
  initial_pages_to_fetch = list(pages_db.aggregate([{'$sample': {'size': num_seed_pages}}]))
  processed_pages = process_seed_pages(pages_db, initial_pages_to_fetch, depth=1)
  print('Processing WP pages')

  connection = pymysql.connect(host=DATABASE_HOST,
                               user=DATABASE_USER,
                               password=DATABASE_PASSWORD,
                               db=DATABASE_NAME,
                               charset='utf8mb4',
                               use_unicode=True,
                               cursorclass=pymysql.cursors.DictCursor)
  try:
    with connection.cursor() as cursor:
      cursor.execute("SET NAMES utf8mb4;")
      cursor.execute("SET CHARACTER SET utf8mb4;")
      cursor.execute("SET character_set_connection=utf8mb4;")
      print('Inserting processed pages')
      source = 'wikipedia'
      for processed_page in progressbar(processed_pages):
        insert_wp_page(cursor, processed_page, source)
        connection.commit()
        insert_category_associations(cursor, processed_page, source)
        connection.commit()
        insert_link_contexts(cursor, processed_page, source)
        connection.commit()
  finally:
    connection.close()


if __name__ == "__main__": main()
