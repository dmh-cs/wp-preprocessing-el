import pydash as _
from functools import reduce
import re

import utils as u

from parsers import parse_for_sentences, parse_text_for_tokens

mention_start_token = 'MENTION_START_HERE'
mention_end_token = 'MENTION_END_HERE'

def _insert_mention_flags(page_content, link_context):
  assert link_context['offset'] < len(page_content)
  start_mention = ' ' + mention_start_token + ' '
  end_mention = ' ' + mention_end_token + ' '
  mention_text = link_context['text']
  start = link_context['offset']
  end = start + len(mention_text)
  content = page_content[:start] + start_mention + mention_text + end_mention + page_content[end:]
  return content

def _sentence_is_unbalanced(sentence):
  start_indexes = u.match_all(mention_start_token, sentence)
  end_indexes = u.match_all(mention_end_token, sentence)
  return len(start_indexes) != len(end_indexes)

def _merge_sentences_with_straddling_mentions(sentences):
  result = []
  sentence_ctr = 0
  while sentence_ctr < len(sentences):
    sentence = sentences[sentence_ctr]
    if _sentence_is_unbalanced(sentence):
      sentence_ctr += 1
      next_sentence = sentences[sentence_ctr]
      transformed_sentence = sentence + ' ' + next_sentence
      while _sentence_is_unbalanced(transformed_sentence):
        sentence_ctr += 1
        next_sentence = sentences[sentence_ctr]
        transformed_sentence = transformed_sentence + ' ' + next_sentence
      sentence_ctr += 1
      result.append(transformed_sentence)
    else:
      sentence_ctr += 1
      result.append(sentence)
  return result

def get_page_iobes(page, mentions, mention_link_titles):
  """Returns a list of triples/pairs describing the iobes of the page
based on `mentions` and `mention_link_titles`. ASSUMES `mentions` and
`mention_link_titles` are in the same order. An element is a pair if
the token is not part of a mention, and is a triple otherwise.
  """
  page_iobes = []
  page_content = page['content']
  flagged_page = reduce(_insert_mention_flags,
                        sorted(mentions, key=lambda pair: pair['offset'], reverse=True),
                        page_content)
  sentences = parse_for_sentences(flagged_page)
  sentences = _merge_sentences_with_straddling_mentions(sentences)
  link_title_ctr = 0
  in_a_mention = False
  for sentence in sentences:
    sentence_tokens = parse_text_for_tokens(sentence)
    sentence_iobes = []
    for token_ctr, current_token in enumerate(sentence_tokens):
      previous_token = sentence_tokens[token_ctr - 1] if token_ctr != 0 else None
      next_token = sentence_tokens[token_ctr + 1] if token_ctr + 1 != len(sentence_tokens) else None
      if current_token == mention_start_token or current_token == mention_end_token:
        continue
      elif previous_token == mention_start_token and next_token == mention_end_token:
        iobes = 'S'
      elif previous_token == mention_start_token:
        iobes = 'B'
        in_a_mention = True
      elif next_token == mention_end_token:
        iobes = 'E'
        in_a_mention = False
      elif in_a_mention:
        iobes = 'I'
      else:
        iobes = 'O'
      if iobes == 'O':
        sentence_iobes.append([current_token, iobes])
      else:
        sentence_iobes.append([current_token, u.escape_title(mention_link_titles[link_title_ctr]), iobes])
        if iobes in ['S', 'E']:
          link_title_ctr += 1
    page_iobes.append(sentence_iobes)
  return page_iobes

def write_page_iobes(page, page_iobes):
  content_to_write = page['title'] + '\n'
  page_content = []
  for sentence_iobes in page_iobes:
    sentence_content = []
    for iobes_chunk in sentence_iobes:
      sentence_content.append(' '.join(iobes_chunk))
    page_content.append('\n'.join(sentence_content))
  content_to_write += '\n\n'.join(page_content)
  with open('./out/' + re.sub(r'\W+', '', page['title']) + '.iobes', 'w') as f:
    f.write(content_to_write)
