# Preprocessing of Wikipedia for entity linking
## Requirements
- mongodb
- mysql
- npm
- python 3

## Setup
- `npm install`
- Make sure the path to the xml dump in `dump_to_mongo.js` is correct
- Start mongo if it isn't already running: `mongod --dbpath <path to data directory>`
- Start mysql if it isn't already running
- `node dump_to_mongo.js` to parse the wikipedia dump and load mongo
- `pip install --user -r requirements.txt` or however you like to install dependencies
- create `db/.env` with the following variables updated for your system:

``` shell
DBNAME=el # desired mysql database name
DBUSER=root # mysql user to authenticate as
DBPASS=pass # password for mysql user
DBHOST=localhost # mysql host
```

## Running
- `cd src`
- `python create_entity_to_context.py`
