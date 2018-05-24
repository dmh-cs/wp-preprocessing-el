from pymongo import MongoClient
from functools import reduce

def sentence_to_link_contexts(sentence):
  contexts = {}
  for link in sentence['links']:
    contexts[link['page']] = {'entity': link['page'],
                              'mention': link['text'],
                              'sentence': sentence['text']}
  return contexts

def sentence_to_link_contexts_reducer(contexts_acc, sentence):
  if sentence.get('links'):
    contexts = sentence_to_link_contexts(sentence)
    contexts_acc.update(contexts)
    return contexts_acc
  else:
    return contexts_acc

def get_link_contexts(sections):
  sentences = sum([section['sentences'] for section in sections], [])
  return reduce(sentence_to_link_contexts_reducer, sentences, {})


def process_page(page):
  document_info = {'title': page['title'],
                   'desc': page['plaintext'][:100],
                   'categories': page['categories']}
  link_contexts = get_link_contexts(page['sections'])
  return {'document_info': document_info, 'link_contexts': link_contexts}

def save_processed_page(client, processed_page):
  pass

def main():
  client = MongoClient()
  dbname = 'afwiki'
  db = client[dbname]
  pages = db['pages']
  initial_pages_to_fetch = []
  for page_name in initial_pages_to_fetch:
    processed_page = process_page(pages.find_one({'_id': page_name}))
    save_processed_page(client, processed_page)

if __name__ == "__main__": main()
