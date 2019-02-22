import pydash as _
import os
import pymysql.cursors
from dotenv import load_dotenv
import pickle
from collections import defaultdict
from progressbar import progressbar

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
      cursor.execute('select mention, entity_id, count(*) as c from entity_mentions_text group by mention, entity_id')
      candidates_prior = defaultdict(lambda: defaultdict(int))
      entity_labels = {}
      for row in progressbar(cursor.fetchall()):
        if row['entity_id'] not in entity_labels:
          entity_labels[row['entity_id']] = len(entity_labels)
        entity_label = entity_labels[row['entity_id']]
        candidates_prior[row['mention']][entity_label] += row['c']

      cursor.execute('select distinct entity_id, entity from entity_mentions_text')
      for row in progressbar(cursor.fetchall()):
        if row['entity_id'] not in entity_labels:
          entity_labels[row['entity_id']] = len(entity_labels)
        entity_label = entity_labels[row['entity_id']]
        candidates_prior[row['entity']][entity_label] += 1

      cursor.execute('select distinct preredirect, entity_id from mentions m join entity_mentions em on em.mention_id = m.id')
      for row in progressbar(cursor.fetchall()):
        if row['entity_id'] not in entity_labels:
          entity_labels[row['entity_id']] = len(entity_labels)
        entity_label = entity_labels[row['entity_id']]
        candidates_prior[row['preredirect']][entity_label] += 1
      with open('lookups.pkl', 'wb') as lookup_file:
        pickle.dump({'lookups': {'entity_candidates_prior': _.map_values(dict(candidates_prior), dict),
                                 'entity_labels': entity_labels},
                     'train_size': 1.0},
                    lookup_file)
  finally:
    connection.close()


if __name__ == "__main__": main()
