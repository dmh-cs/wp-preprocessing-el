import os
import pymysql.cursors
from dotenv import load_dotenv
from progressbar import progressbar
from pymongo import MongoClient
import json

import sys
sys.path.append('./src')

from db import insert_wp_page, insert_category_associations, insert_link_contexts, entity_has_page
from process_pages import process_seed_pages
from lookups import get_redirects_lookup

with open('./data/initial_seed.json') as f:
  ids_to_fetch = json.load(f)


def main():
  load_dotenv(dotenv_path='.env')
  EL_DATABASE_NAME = os.getenv("EL_DBNAME")
  ENWIKI_DATABASE_NAME = os.getenv("ENWIKI_DBNAME")
  DATABASE_USER = os.getenv("DBUSER")
  DATABASE_PASSWORD = os.getenv("DBPASS")
  DATABASE_HOST = os.getenv("DBHOST")

  client = MongoClient()
  dbname = 'enwiki'
  print('Reading from mongodb db', dbname)
  db = client[dbname]
  pages_db = db['pages']
  num_seed_pages = len(ids_to_fetch)
  print('Fetching WP pages using', num_seed_pages, 'seed pages')
  # initial_pages_to_fetch = list(pages_db.find({'_id': {'$in': ids_to_fetch}}))
  initial_pages_to_fetch = list(pages_db.aggregate([{'$sample': {'size': num_seed_pages}}]))
  print('Building redirects lookup')
  redirects_lookup = get_redirects_lookup()
  print('Processing WP pages')
  processed_pages = process_seed_pages(pages_db, redirects_lookup, initial_pages_to_fetch, depth=1)
  client.close()
  el_connection = pymysql.connect(host=DATABASE_HOST,
                                  user=DATABASE_USER,
                                  password=DATABASE_PASSWORD,
                                  db=EL_DATABASE_NAME,
                                  charset='utf8mb4',
                                  use_unicode=True,
                                  cursorclass=pymysql.cursors.DictCursor)
  enwiki_connection = pymysql.connect(host=DATABASE_HOST,
                                      user=DATABASE_USER,
                                      password=DATABASE_PASSWORD,
                                      db=ENWIKI_DATABASE_NAME,
                                      charset='utf8mb4',
                                      use_unicode=True,
                                      cursorclass=pymysql.cursors.DictCursor)
  try:
    with el_connection.cursor() as el_cursor:
      el_cursor.execute("SET NAMES utf8mb4;")
      el_cursor.execute("SET CHARACTER SET utf8mb4;")
      el_cursor.execute("SET character_set_connection=utf8mb4;")
      with enwiki_connection.cursor() as enwiki_cursor:
        enwiki_cursor.execute("SET NAMES utf8mb4;")
        enwiki_cursor.execute("SET CHARACTER SET utf8mb4;")
        enwiki_cursor.execute("SET character_set_connection=utf8mb4;")
        print('Inserting processed pages')
        source = 'wikipedia'
        for processed_page in progressbar(processed_pages):
          if not entity_has_page(enwiki_cursor, processed_page['document_info']['title']):
            continue
          insert_wp_page(el_cursor, processed_page, source)
          el_connection.commit()
          # insert_category_associations(el_cursor, processed_page, source)
          # el_connection.commit()
          insert_link_contexts(enwiki_cursor, el_cursor, processed_page, source)
          el_connection.commit()
  finally:
    el_connection.close()
    enwiki_connection.close()

if __name__ == "__main__":
  import ipdb
  import traceback
  import sys

  try:
    main()
  except: # pylint: disable=bare-except
    extype, value, tb = sys.exc_info()
    traceback.print_exc()
    ipdb.post_mortem(tb)
