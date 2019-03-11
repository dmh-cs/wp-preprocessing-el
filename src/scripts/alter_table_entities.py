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
                               cursorclass=pymysql.cursors.SSDictCursor)
  try:
    with connection.cursor() as cursor:
      cursor.execute("SET NAMES utf8mb4;")
      cursor.execute("SET CHARACTER SET utf8mb4;")
      cursor.execute("SET character_set_connection=utf8mb4;")
      cursor.execute('alter table entities add num_mentions bigint(20)')
      cursor.execute('update entities set num_mentions = (select count(entity_id) as c from mentions m join entity_mentions em on m.id = em.mention_id where em.entity_id = entities.id group by em.entity_id)')
  finally:
    connection.close()


if __name__ == "__main__": main()
