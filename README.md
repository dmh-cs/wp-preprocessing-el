# Preprocessing of Wikipedia for entity linking
## Requirements
- mongodb
- mysql
- npm
- python 3
- wikipedia `page-articles` xml dump
- wikipedia `pages` sql dump
- wikipedia `redirect` sql dump
- `pv` is nice to have for loading sql dumps into local mysql

## Setup
- `npm install`
- Make sure the path to the xml dump in `dump_to_mongo.js` is correct
- Start mongo if it isn't already running: `mongod --dbpath <path to data directory>`
- Start mysql if it isn't already running
- Create a database to restore the wikipedia dumps into: `mysql -u MYSQL_DB_USERNAME -e "CREATE DATABASE IF NOT EXISTS enwiki"`
- Load the wikipedia dumps:
  - `pv enwiki-20180520-page.sql.gz | gunzip | mysql -D enwiki -u MYSQL_DB_USERNAME -p`
  - `pv enwiki-20180520-redirect.sql.gz | gunzip | mysql -D enwiki -u MYSQL_DB_USERNAME -p`
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
- `python src/scripts/create_entity_to_context.py`
- `python src/scripts/create_iobes_training_set.py`
