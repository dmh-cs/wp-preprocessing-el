def upsert_entity(cursor, entity):
  cursor.execute("UPSERT INTO `entities` (`text`) VALUES (%s)", entity)

def insert_wp_page(cursor, processed_page):
  cursor.execute("INSERT INTO `pages` (`source_id`, `title`, `content`, `source`) VALUES (%s, %s, %s, %s)",
                 processed_page['document_info']['source_id'],
                 processed_page['document_info']['title'],
                 processed_page['document_info']['plaintext'],
                 "wikipedia")

def get_page_id_from_source_id(cursor, source, source_page_id):
  return cursor.execute("SELECT `id` FROM `pages` WHERE source_id == %s AND source == %s",
                        source_page_id,
                        source).fetchone()

def upsert_page_category(cursor, source_page_id, category, source):
  page_id = get_page_id_from_source_id(cursor, source, source_page_id)
  cursor.execute("UPSERT INTO `categories` (`category`, `page_id`) VALUES (%s, %s)",
                 category,
                 page_id)

def insert_category_associations(cursor, processed_page, source):
  for category in processed_page['categories']:
    upsert_page_category(cursor, processed_page['source_id'], category, source)

def insert_processed_wp_page(cursor, processed_page):
  insert_wp_page(cursor, processed_page)
  insert_category_associations(cursor, processed_page, 'wikipedia')

def get_entity_id(cursor, entity):
  return cursor.execute("SELECT `id` FROM `entities` WHERE text == %s", entity).fetchone()

def insert_mention(cursor, entity, mention, source_page_id):
  entity_id = get_entity_id(cursor, entity)
  cursor.execute("INSERT INTO `mentions` (`text`, `offset`, `page_id`) VALUES (%s, %s, %s)",
                 mention['text'],
                 mention['offset'],
                 source_page_id)
  cursor.execute("INSERT INTO `entity_mentions` (`entity_id`, `mention_id`), VALUES (%s, LAST_INSERT_ID())",
                 entity_id)

def insert_link_contexts(cursor, processed_page):
  source_page_id = processed_page['document_info']['id']
  for entity, mentions in processed_page['link_contexts'].iteritems():
    upsert_entity(cursor, entity)
    for mention in mentions:
      insert_mention(cursor, entity, mention, source_page_id)
