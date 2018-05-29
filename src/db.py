def _upsert_entity(cursor, entity):
  cursor.execute("INSERT INTO `entities` (`text`) VALUES (%s) ON DUPLICATE KEY UPDATE `text`=VALUES(`text`)",
                 (entity))

def _get_page_id_from_source_id(cursor, source, source_page_id):
  cursor.execute("SELECT `id` FROM `pages` WHERE source_id = %s AND source = %s",
                 (int(source_page_id), source))
  return cursor.fetchone()['id']

def _upsert_page_category(cursor, source_page_id, category, source):
  page_id = _get_page_id_from_source_id(cursor, source, source_page_id)
  cursor.execute("INSERT INTO `categories` (`category`, `page_id`) VALUES (%s, %s) ON DUPLICATE KEY UPDATE `category`=VALUES(`category`), `page_id`=VALUES(`page_id`)",
                 (category, page_id))

def _get_entity_id(cursor, entity):
  cursor.execute("SELECT `id` FROM `entities` WHERE text = %s", (entity))
  return cursor.fetchone()['id']

def _insert_mention(cursor, entity, mention, source_page_id, source):
  entity_id = _get_entity_id(cursor, entity)
  page_id = _get_page_id_from_source_id(cursor, source, source_page_id)
  cursor.execute("INSERT INTO `mentions` (`text`, `offset`, `page_id`) VALUES (%s, %s, %s)",
                 (mention['text'],
                  mention['offset'],
                  page_id))
  cursor.execute("INSERT INTO `entity_mentions` (`entity_id`, `mention_id`) VALUES (%s, LAST_INSERT_ID())",
                 (int(entity_id)))

def insert_category_associations(cursor, processed_page, source):
  for category in processed_page['document_info']['categories']:
    _upsert_page_category(cursor, processed_page['document_info']['source_id'], category, source)

def insert_wp_page(cursor, processed_page, source):
  cursor.execute("INSERT INTO `pages` (`source_id`, `title`, `content`, `source`) VALUES (%s, %s, %s, %s)",
                 (int(processed_page['document_info']['source_id']),
                  processed_page['document_info']['title'],
                  processed_page['document_info']['text'],
                  source))

def insert_link_contexts(cursor, processed_page, source):
  source_page_id = processed_page['document_info']['source_id']
  for entity, mentions in processed_page['link_contexts'].items():
    _upsert_entity(cursor, entity)
    for mention in mentions:
      _insert_mention(cursor, entity, mention, source_page_id, source)
