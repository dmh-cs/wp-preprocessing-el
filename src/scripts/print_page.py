import pydash as _
import os
import pymysql.cursors
from dotenv import load_dotenv

import sys
sys.path.append('./src')

from db import get_page_and_mentions_by_entity
import pretty_printers


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
      page, mentions_by_entity = get_page_and_mentions_by_entity(cursor, int(sys.argv[1]))
      pretty_printers.page_contents_with_mentions(page['content'], mentions_by_entity)
  finally:
    connection.close()


if __name__ == "__main__": main()
