import pymysql
import os
from dotenv import load_dotenv
import pickle
from random import shuffle

import pydash as _
import sys
sys.path.append('./src')

def get_connection():
  load_dotenv(dotenv_path='.env')
  DATABASE_NAME = os.getenv("EL_DBNAME")
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

def get_page_id_order(cursor):
  cursor.execute('select id from pages')
  page_ids = []
  while True:
    results = cursor.fetchmany(10000)
    if _.is_empty(results): break
    page_ids.extend([row['id'] for row in results])
  shuffle(page_ids)
  return page_ids

def main():
  db_connection = get_connection()
  with db_connection.cursor() as cursor:
    page_id_order = get_page_id_order(cursor)
  with open('page_id_order.pkl', 'wb+') as f:
    pickle.dump(page_id_order, f)

if __name__ == "__main__": main()
