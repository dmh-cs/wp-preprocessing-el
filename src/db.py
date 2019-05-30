import pydash as _
from unidecode import unidecode
from utils import build_cursor_generator

def entity_has_page(wiki_titles, entity):
  return unidecode(entity).lower() in wiki_titles

def get_wiki_titles(enwiki_cursor):
  enwiki_cursor.execute("SELECT page_title FROM page WHERE page_is_redirect = 0 and page_namespace = 0")
  return set(unidecode(row['page_title'].replace('_', ' ')).lower() for row in enwiki_cursor.fetchall())

class Inserter():
  def __init__(self, cursor):
    self.cursor = cursor
    self.entity_id_lookup = {}
    self.entity_insert_buffer = []
    self.mention_insert_buffer = []
    self.assoc_insert_buffer = []
    self.num_mentions = 0

  def _bulk_insert_entities(self):
    values = str(self.entity_insert_buffer)[1:-1]
    self.cursor.execute('insert into entities (id, text) values ' + values)
    self.entity_insert_buffer = []

  def _bulk_insert_mentions(self):
    values = str(self.mention_insert_buffer)[1:-1]
    assoc_values = str(self.assoc_insert_buffer)[1:-1]
    if len(self.entity_insert_buffer) != 0: self._bulk_insert_entities()
    self.cursor.execute('insert into mentions (id, text, offset, page_id, preredirect) values ' + values)
    self.cursor.execute('insert into entity_mentions (entity_id, mention_id) values ' + assoc_values)
    self.mention_insert_buffer = []
    self.assoc_insert_buffer = []

  def insert_entity(self, entity):
    if unidecode(entity).lower() not in self.entity_id_lookup:
      entity_id = len(self.entity_id_lookup) + 1
      self.entity_insert_buffer.append((entity_id, unidecode(entity).lower()))
      self.entity_id_lookup[unidecode(entity).lower()] = entity_id
      if len(self.entity_insert_buffer) == 1000:
        self._bulk_insert_entities()
    else:
      entity_id = self.entity_id_lookup[unidecode(entity).lower()]
    return entity_id

  def insert_mention(self, mention, entity_id, page_id):
    mention_id = self.num_mentions + 1
    self.mention_insert_buffer.append((mention_id,
                                       mention['text'],
                                       mention['offset'],
                                       page_id,
                                       mention['preredirect']))
    self.assoc_insert_buffer.append((int(entity_id), mention_id))
    if len(self.mention_insert_buffer) == 1000:
      self._bulk_insert_mentions()
    self.num_mentions += 1

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

def in_batches(cursor, query, buff_len=10000):
  offset = 0
  while True:
    to_exec = query + f' LIMIT {offset}, {buff_len}'
    cursor.execute(to_exec)
    results = cursor.fetchall()
    if not results: return
    for result in results: yield result
    offset += buff_len

def get_nondisambiguation_pages(cursor):
  return in_batches(cursor, "SELECT * FROM pages WHERE is_disambiguation_page = 0")

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
  cursor.execute("REPLACE INTO `pages` (`source_id`, `title`, `content`, `source`, `is_seed_page`, `is_disambiguation_page`) VALUES (%s, %s, %s, %s, %s, %s)",
                 (int(processed_page['document_info']['source_id']),
                  processed_page['document_info']['title'],
                  processed_page['document_info']['text'],
                  source,
                  processed_page['document_info']['is_seed_page'],
                  processed_page['document_info']['is_disambiguation_page']))

def insert_link_contexts(wiki_titles, el_cursor, inserter, processed_page, source):
  source_page_id = processed_page['document_info']['source_id']
  page_id = _get_page_id_from_source_id(el_cursor, source, source_page_id)
  for entity, mentions in processed_page['link_contexts'].items():
    if entity_has_page(wiki_titles, entity):
      entity_id = inserter.insert_entity(entity)
      for mention in mentions:
        inserter.insert_mention(mention, entity_id, page_id)
    else:
      continue

def get_page_and_mentions_by_entity(cursor, page_id):
  cursor.execute("SELECT * from pages WHERE id = (%s)", (page_id))
  page = cursor.fetchone()
  cursor.execute("SELECT * from mention_by_entity WHERE page_id = (%s) ORDER BY offset", (page_id))
  mentions_by_entity = cursor.fetchall()
  return page, mentions_by_entity
