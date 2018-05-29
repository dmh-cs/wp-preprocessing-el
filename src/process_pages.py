from functools import reduce
import pydash as _

def is_valid_page(page):
  flags = ['.jpg', '.svg', '.png', '.gif', '.jpeg', '.bmp', '.tiff', '(disambiguation)']
  if page and page.get('title'):
    return not any([_.strings.has_substr(page['title'].lower(), flag) for flag in flags])
  else:
    return False

def is_valid_link(link):
  flags = ['.jpg', '.svg', '.png', '.gif', '.jpeg', '.bmp', '.tiff']
  if link and link.get('text') and link.get('page'):
    valid_text = not any([_.strings.has_substr(link['text'].lower(), flag) for flag in flags])
    valid_page = not any([_.strings.has_substr(link['page'].lower(), flag) for flag in flags])
    return valid_text and valid_page
  else:
    return False

def process_seed_pages(pages_db, seed_pages, depth=1):
  if depth != 1: raise NotImplementedError('Depth other than 1 not implemented yet')
  visited_page_titles = [page['title'] for page in seed_pages]
  processed_pages = [process_page(page) for page in seed_pages if is_valid_page(page)]
  link_names = sum([list(processed_page['link_contexts'].keys()) for processed_page in processed_pages],
                   [])
  pages_referenced = list(link_names)
  for page_title in _.arrays.difference(pages_referenced, visited_page_titles):
    page = pages_db.find_one({'_id': page_title})
    if is_valid_page(page):
      processed_pages.append(process_page(page))
  return processed_pages

def get_mention_offset(page_text, sentence_text, mention):
  try:
    sentence_page_offset = page_text.index(sentence_text)
  except ValueError:
    raise ValueError('Sentence not found in page')
  try:
    mention_sentence_offset = sentence_text.index(mention)
  except ValueError:
    raise ValueError('Mention not found in sentence')
  return sentence_page_offset + mention_sentence_offset

def sentence_to_link_contexts(page, sentence):
  page_title = page['title']
  contexts = {}
  if sentence.get('links'):
    for link in sentence['links']:
      if link.get('page') and is_valid_link(link):
        try:
          mention_offset = get_mention_offset(page['plaintext'], sentence['text'], link['text'])
          contexts[link['page']] = {'text': link['text'],
                                    'sentence': sentence['text'],
                                    'offset': mention_offset,
                                    'page_title': page_title}
        except ValueError:
          continue
  return contexts

def sentence_to_link_contexts_reducer(page, contexts_acc, sentence):
  contexts = sentence_to_link_contexts(page, sentence)
  if not _.predicates.is_empty(contexts):
    concat = lambda dest, src: dest + [src] if dest else [src]
    _.objects.merge_with(contexts_acc, contexts, iteratee=concat)
  return contexts_acc

def get_link_contexts(page):
  sections = page['sections']
  sentences = sum([section['sentences'] for section in sections], [])
  sentences_from_tables = sum([[table['data'] for table in section['tables'][0]] for section in sections if section.get('tables')],
                              [])
  all_sentences = sentences + sentences_from_tables
  return reduce(_.functions.curry(sentence_to_link_contexts_reducer)(page), all_sentences, {})

def process_page(page):
  document_info = {'source_id': page['pageID'],
                   'title': page['title'],
                   'text': page['plaintext'],
                   'categories': page['categories']}
  link_contexts = get_link_contexts(page)
  entity_counts = _.objects.map_values(link_contexts, len)
  return {'document_info': document_info,
          'link_contexts': link_contexts,
          'entity_counts': entity_counts}

def merge_mentions(processed_pages):
  concat = lambda dest, src: dest + src if dest else src
  link_contexts = reduce(lambda acc, val: _.objects.merge_with(acc, val, iteratee=concat),
                         [processed_page['link_contexts'] for processed_page in processed_pages],
                         {})
  entity_counts = reduce(lambda acc, val: _.objects.merge_with(acc, val, iteratee=concat),
                          [processed_page['entity_counts'] for processed_page in processed_pages],
                          {})
  return _.objects.map_values(link_contexts,
                              lambda val, key: {'link_contexts': val,
                                                'entity_counts': entity_counts[key]})
