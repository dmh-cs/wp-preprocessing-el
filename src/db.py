import pydash as _
from utils import build_cursor_generator

def _insert_entity(cursor, entity):
  cursor.execute("INSERT INTO `entities` (`text`) VALUES (%s) ON DUPLICATE KEY UPDATE id=id",
                 (entity))
  cursor.execute("SELECT id from entities where text = (%s)", (entity))
  return cursor.fetchone()['id']

def _get_page_id_from_source_id(cursor, source, source_page_id):
  cursor.execute("SELECT `id` FROM `pages` WHERE source_id = %s AND source = %s",
                 (int(source_page_id), source))
  return cursor.fetchone()['id']

def _get_entity_id(cursor, entity):
  cursor.execute("SELECT `id` FROM `entities` WHERE text = %s", (entity))
  return cursor.fetchone()['id']

def _get_category_id(cursor, category):
  cursor.execute("SELECT `id` FROM `categories` WHERE category = %s", (category))
  query_result = cursor.fetchone()
  return query_result['id'] if query_result else None

def _insert_mention(cursor, mention, entity_id, page_id):
  cursor.execute("INSERT INTO `mentions` (`text`, `offset`, `page_id`) VALUES (%s, %s, %s)",
                 (mention['text'],
                  mention['offset'],
                  page_id))
  cursor.execute("INSERT INTO `entity_mentions` (`entity_id`, `mention_id`) VALUES (%s, LAST_INSERT_ID())",
                 (int(entity_id)))

def _insert_category(cursor, category):
  cursor.execute("INSERT INTO `categories` (`category`) VALUES (%s) ON DUPLICATE KEY UPDATE id=id",
                 (category))

def _insert_page_category(cursor, page_id, category_id, options):
  if options['use_last_id']:
    cursor.execute("INSERT INTO `page_categories` (`category_id`, `page_id`) VALUES (LAST_INSERT_ID(), %s)",
                   (page_id))
  else:
    cursor.execute("INSERT INTO `page_categories` (`category_id`, `page_id`) VALUES (%s, %s)",
                   (category_id, page_id))

def get_page_ids_with_mentions(cursor):
  cursor.execute("SELECT DISTINCT `page_id` from mentions")
  return [row['page_id'] for row in cursor.fetchall()]

def get_pages_having_mentions(cursor):
  cursor.execute("SELECT * FROM pages WHERE id IN (SELECT DISTINCT page_id FROM mentions)")
  return build_cursor_generator(cursor), cursor.rowcount

def get_page_mentions(cursor, page_id):
  cursor.execute("SELECT * from mentions WHERE page_id = (%s) ORDER BY offset", (page_id))
  return cursor.fetchall()

def get_page_mentions_by_entity(cursor, page_id):
  cursor.execute("SELECT * from mention_by_entity WHERE page_id = (%s) ORDER BY offset", (page_id))
  return cursor.fetchall()

def get_page_titles(cursor, page_ids):
  cursor.execute("SELECT title FROM pages WHERE id IN (" + _.join(page_ids, ',') + ")")
  return _.pluck(cursor.fetchall(), 'title')

def insert_category_associations(cursor, processed_page, source):
  source_page_id = processed_page['document_info']['source_id']
  page_id = _get_page_id_from_source_id(cursor, source, source_page_id)
  for category in processed_page['document_info']['categories']:
    category_id = _get_category_id(cursor, category)
    if category_id:
      _insert_page_category(cursor, page_id, category_id, {'use_last_id': False})
    else:
      _insert_category(cursor, category)
      _insert_page_category(cursor, page_id, category_id, {'use_last_id': True})

def insert_wp_page(cursor, processed_page, source):
  cursor.execute("REPLACE INTO `pages` (`source_id`, `title`, `content`, `source`) VALUES (%s, %s, %s, %s)",
                 (int(processed_page['document_info']['source_id']),
                  processed_page['document_info']['title'],
                  processed_page['document_info']['text'],
                  source))

def insert_link_contexts(cursor, processed_page, source):
  source_page_id = processed_page['document_info']['source_id']
  page_id = _get_page_id_from_source_id(cursor, source, source_page_id)
  for entity, mentions in processed_page['link_contexts'].items():
    entity_id = _insert_entity(cursor, entity)
    for mention in mentions:
      _insert_mention(cursor, mention, entity_id, page_id)

def get_page_and_mentions_by_entity(cursor, page_id):
  cursor.execute("SELECT * from pages WHERE id = (%s)", (page_id))
  page = cursor.fetchone()
  cursor.execute("SELECT * from mention_by_entity WHERE page_id = (%s) ORDER BY offset", (page_id))
  mentions_by_entity = cursor.fetchall()
  return page, mentions_by_entity
