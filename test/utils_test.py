from unittest.mock import Mock
import pydash as _

import utils as u

def get_fetchmany_stub(max_count):
  count = 0
  def _fetchmany_stub(num_rows):
    nonlocal count
    if count < max_count:
      count += 1
      return range(num_rows)
    else: return None
  return _fetchmany_stub

def test_build_cursor_generator():
  cursor = Mock()
  cursor.fetchmany = get_fetchmany_stub(3)
  gen = u.build_cursor_generator(cursor, 1000)
  iteration_counter = 0
  for x in gen:
    iteration_counter += 1
  assert iteration_counter == 3 * 1000
