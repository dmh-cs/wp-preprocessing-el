include ../.env

up:
	@echo ''
	@echo '-------------------'
	@echo '-- Creating database `$(ENWIKI_DBNAME)` --'
	@echo '-------------------'
	@echo ''
	mysql -u"$(DBUSER)" --host "$(DBHOST)" -e "CREATE DATABASE IF NOT EXISTS $(ENWIKI_DBNAME)"

	@echo ''
	@echo '-------------------'
	@echo '-- Loading page table --'
	@echo '-------------------'
	@echo ''
	pv enwiki-20180520-page.sql.gz | gunzip | mysql -u"$(DBUSER)" --host "$(DBHOST)" "$(ENWIKI_DBNAME)"

	@echo ''
	@echo '-------------------'
	@echo '-- Loading redirect table --'
	@echo '-------------------'
	@echo ''
	pv enwiki-20180520-redirect.sql.gz | gunzip | mysql -u"$(DBUSER)" --host "$(DBHOST)" "$(ENWIKI_DBNAME)"

	@echo ''
	@echo '-------------------'
	@echo '-- Creating page title index --'
	@echo '-------------------'
	@echo ''
	mysql -u"$(DBUSER)" --host "$(DBHOST)" "$(ENWIKI_DBNAME)" < create_page_title_index.sql

down:
	@echo ''
	@echo '-------------------'
	@echo '-- Dropping tables --'
	@echo '-------------------'
	@echo ''
	mysql -u"$(DBUSER)" --host "$(DBHOST)" "$(ENWIKI_DBNAME)" < drop_tables.sql

.PHONY: help el
