import pydash as _
import json
import sys
sys.path.append('./test/fixtures')
from parade_iobes import parade_iobes

import iobes


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

def test_get_page_iobes():
  with open('test/fixtures/parade_page_db.json') as f:
    parade_page = json.load(f)
  with open('test/fixtures/parade_page_contexts.json') as f:
    parade_page_contexts = json.load(f)
  context_pairs = _.mapcat(_.objects.to_pairs(parade_page_contexts),
                           lambda pair: [[pair[0], mention] for mention in pair[1]])
  contexts = _.collections.sort_by(context_pairs,
                                   lambda title_mention: title_mention[1]['offset'])
  mentions = _.collections.flat_map(contexts, _.arrays.last)
  mention_link_titles = list(map(_.arrays.head, contexts))
  assert _.predicates.is_equal(parade_iobes,
                               iobes.get_page_iobes(parade_page,
                                                    mentions,
                                                    mention_link_titles))
