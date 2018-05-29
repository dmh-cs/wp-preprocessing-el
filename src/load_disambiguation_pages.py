def fetch_disambiguation_pages(pages_db):
  return pages_db.find({"title": {"$regex": "(disambiguation)"}})

def main():
  raise NotImplementedError()


if __name__ == "__main__": main()
