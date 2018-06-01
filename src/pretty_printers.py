import pydash as _
import chalk

from utils import sort_mentions

def page_iobes(page_iobes):
  chalked_sentence_iobes = []
  for sentence_iobes in page_iobes:
    for iobes in sentence_iobes:
      if len(iobes) == 3:
        chalked_sentence_iobes.append(chalk.green(' '.join(iobes)))
      else:
        chalked_sentence_iobes.append(' '.join(iobes))
  print('\n'.join(chalked_sentence_iobes))

def page_contents_with_mentions(page_contents, mentions_by_entity):
  chalked_contents = page_contents
  sorted_mentions = sort_mentions(mentions_by_entity)
  for mention in reversed(sorted_mentions):
    start = mention['offset']
    end = mention['offset'] + len(mention['text'])
    chalked_contents = chalked_contents[: start] + chalk.green(page_contents[start : end]) + chalk.cyan(mention['entity'], underline=True) + chalked_contents[end:]
  print(chalked_contents)
