import pydash as _
import os
import pymysql.cursors
from dotenv import load_dotenv
from progressbar import progressbar

import sys
sys.path.append('./src')

from db import get_page_mentions_by_entity, get_nondisambiguation_pages, get_page_titles
from iobes import get_page_iobes, write_page_iobes


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
    with connection.cursor() as pages_cursor:
      pages_cursor.execute("SET NAMES utf8mb4;")
      pages_cursor.execute("SET CHARACTER SET utf8mb4;")
      pages_cursor.execute("SET character_set_connection=utf8mb4;")
      with connection.cursor() as mentions_cursor:
        mentions_cursor.execute("SET NAMES utf8mb4;")
        mentions_cursor.execute("SET CHARACTER SET utf8mb4;")
        mentions_cursor.execute("SET character_set_connection=utf8mb4;")
        pages = get_nondisambiguation_pages(pages_cursor)
        for page in progressbar(pages):
          page_id = page['id']
          sorted_mentions = get_page_mentions_by_entity(mentions_cursor, page_id)
          if len(sorted_mentions) == 0: continue
          mention_link_titles = _.pluck(sorted_mentions, 'entity')
          mention_link_titles_preredirect = _.pluck(sorted_mentions, 'preredirect')
          page_iobes = get_page_iobes(page,
                                      sorted_mentions,
                                      mention_link_titles,
                                      mention_link_titles_preredirect)
          write_page_iobes(page, page_iobes)
  finally:
    connection.close()


if __name__ == "__main__": main()
