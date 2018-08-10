# Preprocessing of Wikipedia for entity linking
## Requirements
- mongodb
- mysql
- npm
- python 3
- wikipedia `page-articles` xml dump
- wikipedia `pages` sql dump
- wikipedia `redirect` sql dump
- `pv` is nice to have for loading sql dumps into local mysql (`brew install pv`)

## Setup
- `npm install`
- Make sure the path to the xml dump in `dump_to_mongo.js` is correct
- Start mongo if it isn't already running: `mongod --dbpath <path to data directory>`
- Start mysql if it isn't already running
- `node dump_to_mongo.js` to parse the wikipedia dump and load it into mongo
- `pip install --user -r requirements.txt` or however you like to install dependencies
- Create `.env` with the following variables updated for your system:
``` shell
ENWIKI_DBNAME=enwiki # desired mysql database name for wikipedia dumps
EL_DBNAME=el # desired mysql database name
DBUSER=root # mysql user to authenticate as
DBPASS=pass # password for mysql user
DBHOST=localhost # mysql host
```
- Fetch the nltk requirements:
``` python
import nltk
nltk.download('punkt')
```
- Load the wikipedia dumps:
  - `cd enwiki_db`
  - `make`
- Setup the database for creating the entity linking dataset:
  - `cd db`
  - `make`

## Running
- `cd src`
- `python src/scripts/create_entity_to_context.py` loads the MySQL db.
- `python src/scripts/create_iobes_training_set.py` creates iobes files for NER. Written to `out/`.
- `python src/scripts/create_page_id_order.py` creates a pickle file defined a randomized ordering of pages to iterate through. This is needed so that the train-test split can be established before creating the prior distribution over entities.
- `python src/scripts/create_entity_prior.py` creates the lookup for labels and prior over the entities.

## Tests
- `pytest test`
