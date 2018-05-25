import pprint
from pymongo import MongoClient
from functools import reduce
import pydash as _

def is_valid_page(page):
  extensions = ['.jpg', '.svg', '.png', '.gif', '.jpeg', '.bmp', '.tiff']
  if page and page.get('title'):
    return not any([_.strings.has_substr(page['title'].lower(), extension) for extension in extensions])
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

def sentence_to_link_contexts(page_title, sentence):
  contexts = {}
  if sentence.get('links'):
    for link in sentence['links']:
      if link.get('page'):
        contexts[link['page']] = {'mention': link['text'],
                                  'sentence': sentence['text'],
                                  'page_title': page_title}
  return contexts

def sentence_to_link_contexts_reducer(page_title, contexts_acc, sentence):
  contexts = sentence_to_link_contexts(page_title, sentence)
  if not _.predicates.is_empty(contexts):
    concat = lambda dest, src: dest + [src] if dest else [src]
    _.objects.merge_with(contexts_acc, contexts, iteratee=concat)
  return contexts_acc

def get_link_contexts(page):
  sections = page['sections']
  page_title = page['title']
  sentences = sum([section['sentences'] for section in sections], [])
  return reduce(_.functions.curry(sentence_to_link_contexts_reducer)(page_title), sentences, {})


def process_page(page):
  document_info = {'title': page['title'],
                   'desc': page['plaintext'][:100],
                   'categories': page['categories']}
  link_contexts = get_link_contexts(page)
  entity_counts = _.objects.map_values(link_contexts, len)
  return {'document_info': document_info,
          'link_contexts': link_contexts,
          'entity_counts': entity_counts}

def save_processed_page(client, processed_page):
  pass

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

def main():
  client = MongoClient()
  dbname = 'afwiki'
  db = client[dbname]
  pages_db = db['pages']
  num_seed_pages = 1000
  initial_pages_to_fetch = list(pages_db.aggregate([{'$sample': {'size': num_seed_pages}}]))
  processed_pages = process_seed_pages(pages_db, initial_pages_to_fetch)
  merged = merge_mentions(processed_pages)
  print('num pages', len(merged.keys()))
  pp = pprint.PrettyPrinter(indent=4)
  pp.pprint(_.objects.map_values(merged, 'entity_counts'))
  # for processed_page in processed_pages:
  #   save_processed_page(client, processed_page)


if __name__ == "__main__": main()
