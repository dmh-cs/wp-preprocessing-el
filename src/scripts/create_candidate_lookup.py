import pydash as _
import os
import pymysql.cursors
from dotenv import load_dotenv
from progressbar import progressbar
import math
import pickle

import sys
sys.path.append('./src')

def main():
  load_dotenv(dotenv_path='.env')
  EL_DATABASE_NAME = os.getenv("EL_DBNAME")
  DATABASE_USER = os.getenv("DBUSER")
  DATABASE_PASSWORD = os.getenv("DBPASS")
  DATABASE_HOST = os.getenv("DBHOST")
  connection = pymysql.connect(host=DATABASE_HOST,
                               user=DATABASE_USER,
                               password=DATABASE_PASSWORD,
                               db=EL_DATABASE_NAME,
                               charset='utf8mb4',
                               use_unicode=True,
                               cursorclass=pymysql.cursors.DictCursor)
  try:
    with connection.cursor() as cursor:
      cursor.execute("SET NAMES utf8mb4;")
      cursor.execute("SET CHARACTER SET utf8mb4;")
      cursor.execute("SET character_set_connection=utf8mb4;")
      cursor.execute('select mention_id, entity_id from entity_mentions')
      buff_len = 10000
      num_batches = math.ceil(cursor.rowcount / buff_len)
      lookup = {}
      for batch_num in progressbar(range(num_batches)):
        results = cursor.fetchmany(buff_len)
        for row in results:
          if row['mention_id'] in lookup:
            lookup[row['mention_id']].append(row['entity_id'])
          else:
            lookup[row['mention_id']] = [row['entity_id']]
      with open('candidate_lookup.pkl', 'wb') as lookup_file:
        pickle.dump(lookup, lookup_file)
  finally:
    connection.close()


if __name__ == "__main__": main()
