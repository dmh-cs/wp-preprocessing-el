# coding: utf-8

import pydash as _
import json
import sys
sys.path.append('./test/fixtures')
from parade_iobes import parade_iobes

import iobes
from utils import escape_title


def test__label_iobes():
  mention_spans = [(0, 3), (7, 15)]
  assert iobes._label_iobes(mention_spans, (0, 3)) == 'S'
  assert iobes._label_iobes(mention_spans, (8, 14)) == 'I'
  assert iobes._label_iobes(mention_spans, (4, 6)) == 'O'
  assert iobes._label_iobes(mention_spans, (3, 6)) == 'O'
  assert iobes._label_iobes(mention_spans, (4, 7)) == 'O'
  assert iobes._label_iobes(mention_spans, (3, 7)) == 'O'
  assert iobes._label_iobes(mention_spans, (8, 15)) == 'E'
  assert iobes._label_iobes(mention_spans, (0, 2)) == 'B'

def test_get_page_iobes_overlapping_matches():
  page = {'source_id': 0, 'title': 'Other', 'content': 'some other text and my stuff'}
  mentions = [{'text': 'some other text', 'offset': 0, 'page_title': 'Other'},
              {'text': 'my', 'offset': 20, 'page_title': 'My page'}]
  mention_link_titles = ['Other', 'My page']
  assert [[['some', 'Other', 'B'],
           ['other', 'Other', 'I'],
           ['text', 'Other', 'E'],
           ['and', 'O'],
           ['my', 'My%20page', 'S'],
           ['stuff', 'O']]] == iobes.get_page_iobes(page, mentions, mention_link_titles)

def test_get_page_iobes():
  with open('test/fixtures/parade_page_db.json') as f:
    parade_page = json.load(f)
  with open('test/fixtures/parade_page_contexts.json') as f:
    parade_page_contexts = json.load(f)
  context_pairs = _.mapcat(_.to_pairs(parade_page_contexts),
                           lambda pair: [[pair[0], mention] for mention in pair[1]])
  contexts = _.sort_by(context_pairs,
                                   lambda title_mention: title_mention[1]['offset'])
  mentions = _.flat_map(contexts, _.last)
  mention_link_titles = list(map(_.head, contexts))
  assert parade_iobes == iobes.get_page_iobes(parade_page, mentions, mention_link_titles)

def test_get_page_iobes_word_match():
  page = {'content': '*2002–03 NHL season', 'source_id': 0, 'title': '2002–03 Buffalo Sabres season'}
  page_contexts = {'2002–03 NHL season': [{'text': '2002–03 NHL season',
                                           'sentence': '*2002–03 NHL season',
                                           'offset': 1,
                                           'page_title': '2002–03 Buffalo Sabres season'}]}
  mentions = list(page_contexts.values())[0]
  mention_link_titles = ['2002–03 NHL season']
  page_iobes = [[['*', 'O'], [escape_title('2002–03'), 'B'], ['NHL', 'I'], ['season', 'E']]]
  assert page_iobes == iobes.get_page_iobes(page, mentions, mention_link_titles)
