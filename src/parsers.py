# coding: utf-8

import re
from nltk.data              import load
from nltk.tokenize.treebank import TreebankWordTokenizer

_treebank_word_tokenizer = TreebankWordTokenizer()

improved_open_quote_regex = re.compile(u'([«“‘„]|[`]+)', re.U)
improved_close_quote_regex = re.compile(u'([»”’])', re.U)
improved_punct_regex = re.compile(r'([^\.])(\.)([\]\)}>"\'' u'»”’ ' r']*)\s*$', re.U)
_treebank_word_tokenizer.STARTING_QUOTES.insert(0, (improved_open_quote_regex, r' \1 '))
_treebank_word_tokenizer.ENDING_QUOTES.insert(0, (improved_close_quote_regex, r' \1 '))
_treebank_word_tokenizer.PUNCTUATION.insert(0, (improved_punct_regex, r'\1 \2 \3 '))

def parse_for_sentence_offsets(page_content):
  tokenizer = load('tokenizers/punkt/{0}.pickle'.format('english'))
  return tokenizer.span_tokenize(page_content)

def parse_for_token_offsets(sentence):
  return _treebank_word_tokenizer.span_tokenize(sentence)
