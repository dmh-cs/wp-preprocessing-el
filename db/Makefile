include .env

up:
	@echo ''
	@echo '-------------------'
	@echo '-- Creating database `el` --'
	@echo '-------------------'
	@echo ''
	mysql -u"$(DBUSER)" --host "$(DBHOST)" -e "CREATE DATABASE IF NOT EXISTS el"

	@echo ''
	@echo '-------------------'
	@echo '-- Creating tables --'
	@echo '-------------------'
	@echo ''
	@sleep 1
	mysql -u"$(DBUSER)" --host "$(DBHOST)" "$(DBNAME)" < create_tables.sql

down:
	@echo ''
	@echo '-------------------'
	@echo '-- Dropping tables --'
	@echo '-------------------'
	@echo ''
	@sleep 1
	mysql -u"$(DBUSER)" --host "$(DBHOST)" "$(DBNAME)" < drop_tables.sql

.PHONY: help el
