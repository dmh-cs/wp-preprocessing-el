import pydash as _
import urllib

def build_cursor_generator(cursor, buff_len=1000):
  while True:
    results = cursor.fetchmany(buff_len)
    if not results: return
    for result in results: yield result

def sort_mentions(mentions):
  return _.sort(mentions, key=lambda obj: obj['offset'])

def match_all(to_match, string):
  assert to_match != ''
  indexes = []
  index = string.find(to_match)
  remaining_string = string
  while index != -1:
    if _.is_empty(indexes):
      indexes.append(index)
    else:
      indexes.append(indexes[-1] + len(to_match) + index)
    remaining_string = remaining_string[index + len(to_match):]
    index = remaining_string.find(to_match)
  return indexes

def create_batches(coll, batch_size=1000):
  yield coll[:batch_size]
  ctr = 1
  while ctr * batch_size < len(coll):
    yield coll[ctr * batch_size : (ctr + 1) * batch_size]
    ctr += 1

def escape_title(title):
  return urllib.parse.quote(title)
