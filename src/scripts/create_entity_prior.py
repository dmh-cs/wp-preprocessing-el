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
      cursor.execute('select mention, entity_id, page_id from entity_mentions_text')
      buff_len = 10000
      num_batches = math.ceil(cursor.rowcount / buff_len)
      lookups = {'entity_candidates_prior': {},
                 'leftover_candidates': {},
                 'entity_labels': {}}
      entity_label_ctr = 0
      train_size = 0.8
      try:
        with open('./page_id_order.pkl', 'rb') as f:
          page_id_order = pickle.load(f)
      except Exception as e:
        raise type(e)(str(e) + '\n' + 'Create `page_id_order.pkl` by running `create_page_id_order.py`').with_traceback(sys.exc_info()[2])
      num_train_pages = int(len(page_id_order) * train_size)
      train_page_id_order = page_id_order[:num_train_pages]
      for batch_num in progressbar(range(num_batches)):
        results = cursor.fetchmany(buff_len)
        for row in results:
          if row['entity_id'] not in lookups['entity_labels']:
            lookups['entity_labels'][row['entity_id']] = entity_label_ctr
            entity_label_ctr += 1
          if row['page_id'] in train_page_id_order:
            property_name = 'entity_candidates_prior'
          else:
            property_name = 'leftover_candidates'
          entity_label = lookups['entity_labels'][row['entity_id']]
          if row['mention'] in lookups[property_name]:
            if entity_label not in lookups[property_name][row['mention']]:
              if entity_label in lookups[property_name][row['mention']]:
                lookups[property_name][row['mention']][entity_label] += 1
              else:
                lookups[property_name][row['mention']][entity_label] = 1
          else:
            lookups[property_name][row['mention']] = {entity_label: 1}
      cursor.execute('select id, text from entities')
      for row in cursor.fetchall():
        if row['id'] not in lookups['entity_labels']: continue
        entity_label = lookups['entity_labels'][row['id']]
        if row['text'] not in lookups['entity_candidates_prior']:
          lookups['entity_candidates_prior'][row['text']] = {}
        if not _.has(lookups['entity_candidates_prior'],
                     [row['text'], entity_label]):
          if entity_label in lookups['entity_candidates_prior'][row['text']]:
            lookups['entity_candidates_prior'][row['text']][entity_label] += 1
          else:
            lookups['entity_candidates_prior'][row['text']][entity_label] = 1
      cursor.execute('select distinct preredirect, entity_id from mentions m join entity_mentions em on em.mention_id = m.id')
      for row in cursor.fetchall():
        if row['entity_id'] not in lookups['entity_labels']: continue
        entity_label = lookups['entity_labels'][row['entity_id']]
        if row['preredirect'] not in lookups['entity_candidates_prior']:
          lookups['entity_candidates_prior'][row['preredirect']] = {}
        if not _.has(lookups['entity_candidates_prior'],
                     [row['preredirect'], entity_label]):
          if entity_label in lookups['entity_candidates_prior'][row['preredirect']]:
            lookups['entity_candidates_prior'][row['preredirect']][entity_label] += 1
          else:
            lookups['entity_candidates_prior'][row['preredirect']][entity_label] = 1
      with open('lookups.pkl', 'wb') as lookup_file:
        pickle.dump({'lookups': lookups, 'train_size': train_size}, lookup_file)
  finally:
    connection.close()


if __name__ == "__main__": main()
