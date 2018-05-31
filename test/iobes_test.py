import pydash as _
import json
import sys
sys.path.append('./test/fixtures')
from parade_iobes import parade_iobes

import iobes


def test__label_iobes():
  mention_start_end_offsets = [(0, 3), (7, 15)]
  assert iobes._label_iobes(mention_start_end_offsets, 0, 3) == 'S'
  assert iobes._label_iobes(mention_start_end_offsets, 8, 14) == 'I'
  assert iobes._label_iobes(mention_start_end_offsets, 4, 6) == 'O'
  assert iobes._label_iobes(mention_start_end_offsets, 3, 6) == 'O'
  assert iobes._label_iobes(mention_start_end_offsets, 4, 7) == 'O'
  assert iobes._label_iobes(mention_start_end_offsets, 3, 7) == 'O'
  assert iobes._label_iobes(mention_start_end_offsets, 8, 15) == 'E'
  assert iobes._label_iobes(mention_start_end_offsets, 0, 2) == 'B'

def test_get_page_iobes():
  with open('test/fixtures/parade_page_db.json') as f:
    parade_page = json.load(f)
  with open('test/fixtures/parade_page_contexts.json') as f:
    parade_page_contexts = json.load(f)
  mentions = _.collections.flat_map(_.objects.to_pairs(parade_page_contexts),
                                    _.arrays.last)
  mention_link_titles = list(map(_.arrays.head,
                                 _.objects.to_pairs(parade_page_contexts)))
  iobes.pretty_print_page_iobes(iobes.get_page_iobes(parade_page,
                                                    mentions,
                                                    mention_link_titles))
  assert _.predicates.is_equal(parade_iobes,
                               iobes.get_page_iobes(parade_page,
                                                    mentions,
                                                    mention_link_titles))
