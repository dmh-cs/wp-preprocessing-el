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
      cursor.execute('select mention, entity_id from entity_mentions_text')
      buff_len = 10000
      num_batches = math.ceil(cursor.rowcount / buff_len)
      lookups = {'entity_candidates': {},
                 'entity_labels': {}}
      entity_label_ctr = 0
      for batch_num in progressbar(range(num_batches)):
        results = cursor.fetchmany(buff_len)
        for row in results:
          if row['entity_id'] not in lookups['entity_labels']:
            lookups['entity_labels'][row['entity_id']] = entity_label_ctr
            entity_label_ctr += 1
          if row['mention'] in lookups['entity_candidates']:
            if lookups['entity_labels'][row['entity_id']] not in lookups['entity_candidates'][row['mention']]:
              lookups['entity_candidates'][row['mention']].append(lookups['entity_labels'][row['entity_id']])
          else:
            lookups['entity_candidates'][row['mention']] = [lookups['entity_labels'][row['entity_id']]]
      with open('lookups.pkl', 'wb') as lookup_file:
        pickle.dump(lookups, lookup_file)
  finally:
    connection.close()


if __name__ == "__main__": main()
