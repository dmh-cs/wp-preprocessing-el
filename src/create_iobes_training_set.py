import pydash as _
import os
import pymysql.cursors
from dotenv import load_dotenv
from pathlib import Path
from progressbar import progressbar

from parsers import parse_for_sentence_offsets, parse_for_token_offsets
from db import get_page_mentions, get_pages_having_mentions

def get_sentence(page_content, sentence_start, sentence_end):
  return page_content[sentence_start : sentence_end]

def write_page_iobes(page, page_iobes):
  with open('./out/' + page + '.iobes', 'w') as f:
    f.write('\n'.join(page_iobes))

def label_iobes(mention_start_end_offsets, token_start, token_end):
  if any([_.predicates.is_equal([token_start, token_end],
                                mention_start_end) for mention_start_end in mention_start_end_offsets]):
    return 'S'
  elif any([token_start == mention_start for mention_start, _ in mention_start_end_offsets]):
    return 'B'
  elif any([token_end == mention_end for _, mention_end in mention_start_end_offsets]):
    return 'E'
  elif any([token_start < mention_start and token_end < mention_end for mention_start, mention_end in mention_start_end_offsets]):
    return 'I'
  else:
    return 'O'

def get_page_iobes(cursor, page):
  page_iobes = []
  page_content = page['content']
  page_id = page['id']
  mentions = get_page_mentions(cursor, page_id)
  for sentence_start, sentence_end in parse_for_sentence_offsets(page_content):
    sentence = get_sentence(page_content, sentence_start, sentence_end)
    [token_start_offsets, token_end_offsets] = [sentence_start + offset for offset in parse_for_token_offsets(sentence)]
    mention_start_end_offsets = [[mention['offset'],
                                  mention['offset'] + mention['text']] for mention in mentions]
    iobes_sequence = [label_iobes(mention_start_end_offsets, start, end) for start, end in zip(token_start_offsets,
                                                                                                     token_end_offsets)]
    page_iobes.append(' '.join([iobes + ' ' + page['title'] if (iobes == 'E' or iobes == 'S') else iobes for iobes in iobes_sequence]))
  return page_iobes


def main():
  load_dotenv(dotenv_path=Path('../db') / '.env')
  DATABASE_NAME = os.getenv("DBNAME")
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
  try:
    with connection.cursor() as cursor:
      cursor.execute("SET NAMES utf8mb4;")
      cursor.execute("SET CHARACTER SET utf8mb4;")
      cursor.execute("SET character_set_connection=utf8mb4;")
      for page in progressbar(get_pages_having_mentions(cursor)):
        page_iobes = get_page_iobes(cursor, page)
        write_page_iobes(page, page_iobes)
  finally:
    connection.close()


if __name__ == "__main__": main()
