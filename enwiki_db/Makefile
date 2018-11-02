include ../.env

up:
	@echo ''
	@echo '-------------------'
	@echo '-- Creating database `$(ENWIKI_DBNAME)` --'
	@echo '-------------------'
	@echo ''
	mysql --port 3306 -p"$(DBPASS)" -u"$(DBUSER)" --host "$(DBHOST)" -e "CREATE DATABASE IF NOT EXISTS $(ENWIKI_DBNAME)"

	@echo ''
	@echo '-------------------'
	@echo '-- Loading page table --'
	@echo '-------------------'
	@echo ''
	pv enwiki-20180520-page.sql.gz | gunzip | mysql --port 3306 -p"$(DBPASS)" -u"$(DBUSER)" --host "$(DBHOST)" "$(ENWIKI_DBNAME)"

	@echo ''
	@echo '-------------------'
	@echo '-- Loading redirect table --'
	@echo '-------------------'
	@echo ''
	pv enwiki-20180520-redirect.sql.gz | gunzip | mysql --port 3306 -p"$(DBPASS)" -u"$(DBUSER)" --host "$(DBHOST)" "$(ENWIKI_DBNAME)"


	@echo ''
	@echo '-------------------'
	@echo '-- Changing `page` charset to utf8mb4 --'
	@echo '-------------------'
	@echo ''
	mysql --port 3306 -p"$(DBPASS)" -u"$(DBUSER)" --host "$(DBHOST)" "$(ENWIKI_DBNAME)" -e "ALTER TABLE page CONVERT TO CHARACTER SET utf8mb4"

	@echo ''
	@echo '-------------------'
	@echo '-- Changing `redirect` charset to utf8mb4 --'
	@echo '-------------------'
	@echo ''
	mysql --port 3306 -p"$(DBPASS)" -u"$(DBUSER)" --host "$(DBHOST)" "$(ENWIKI_DBNAME)" -e "ALTER TABLE redirect CONVERT TO CHARACTER SET utf8mb4"

	@echo ''
	@echo '-------------------'
	@echo '-- Dropping name_title constraint on page table --'
	@echo '-------------------'
	@echo ''
	mysql --port 3306 -p"$(DBPASS)" -u"$(DBUSER)" --host "$(DBHOST)" "$(ENWIKI_DBNAME)" -e "ALTER table page drop index name_title"

	@echo ''
	@echo '-------------------'
	@echo '-- Changing `page_title` type to varchar --'
	@echo '-------------------'
	@echo ''
	mysql --port 3306 -p"$(DBPASS)" -u"$(DBUSER)" --host "$(DBHOST)" "$(ENWIKI_DBNAME)" -e "ALTER TABLE page modify COLUMN page_title VARCHAR(255) NOT NULL DEFAULT ''"

	@echo ''
	@echo '-------------------'
	@echo '-- Changing `rd_title` type to varchar --'
	@echo '-------------------'
	@echo ''
	mysql --port 3306 -p"$(DBPASS)" -u"$(DBUSER)" --host "$(DBHOST)" "$(ENWIKI_DBNAME)" -e "ALTER TABLE redirect modify COLUMN rd_title VARCHAR(255) NOT NULL DEFAULT ''"

	@echo ''
	@echo '-------------------'
	@echo '-- Creating page title index --'
	@echo '-------------------'
	@echo ''
	mysql --port 3306 -p"$(DBPASS)" -u"$(DBUSER)" --host "$(DBHOST)" "$(ENWIKI_DBNAME)" < create_page_title_index.sql

down:
	@echo ''
	@echo '-------------------'
	@echo '-- Dropping tables --'
	@echo '-------------------'
	@echo ''
	mysql --port 3306 -p"$(DBPASS)" -u"$(DBUSER)" --host "$(DBHOST)" "$(ENWIKI_DBNAME)" < drop_tables.sql

.PHONY: help el
