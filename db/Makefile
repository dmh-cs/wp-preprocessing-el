include .env

up:
	@echo ''
	@echo '-------------------'
	@echo '-- Creating database `$(DBNAME)` --'
	@echo '-------------------'
	@echo ''
	mysql -u"$(DBUSER)" --host "$(DBHOST)" -e "CREATE DATABASE IF NOT EXISTS $(DBNAME)"

	@echo ''
	@echo '-------------------'
	@echo '-- Creating tables --'
	@echo '-------------------'
	@echo ''
	@sleep 1
	mysql -u"$(DBUSER)" --host "$(DBHOST)" "$(DBNAME)" < create_tables.sql

	@echo ''
	@echo '-------------------'
	@echo '-- Creating views --'
	@echo '-------------------'
	@echo ''
	@sleep 1
	mysql -u"$(DBUSER)" --host "$(DBHOST)" "$(DBNAME)" < create_views.sql

down:
	@echo ''
	@echo '-------------------'
	@echo '-- Dropping tables --'
	@echo '-------------------'
	@echo ''
	@sleep 1
	mysql -u"$(DBUSER)" --host "$(DBHOST)" "$(DBNAME)" < drop_tables.sql

	@echo ''
	@echo '-------------------'
	@echo '-- Dropping views --'
	@echo '-------------------'
	@echo ''
	@sleep 1
	mysql -u"$(DBUSER)" --host "$(DBHOST)" "$(DBNAME)" < drop_views.sql

.PHONY: help el
