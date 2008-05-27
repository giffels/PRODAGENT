#!/usr/bin/make

PREFIX=$(shell pwd)

# Sources
PYTHON_SRC=$(shell pwd)/src/python
SQL_SRC=$(shell pwd)/src/sql
BIN_SRC=$(shell pwd)/bin

# Targets
LIB_DIR=$(PREFIX)/lib
BIN_DIR=$(PREFIX)/bin
SHARE_DIR=$(PREFIX)/share


.PHONY: all
.PHONY: install

build:
	@cd $(PYTHON_SRC) && python setup.py build --build-lib=$(LIB_DIR)
	/bin/cp $(PYTHON_SRC)/ShREEK/shreek $(LIB_DIR)/ShREEK
	/bin/cp $(PYTHON_SRC)/HWlogging.sh $(LIB_DIR)/
	/bin/cp $(SQL_SRC)/ProdAgentDB/*.sql  $(SHARE_DIR)
	/bin/cp $(PYTHON_SRC)/RssFeeder/*.gif $(LIB_DIR)/RssFeeder
	/bin/chmod +x $(LIB_DIR)/JobCreator/RuntimeTools/*.py
ifneq ($(BIN_DIR), $(BIN_SRC))
	/bin/cp -f $(BIN_SRC)/prodAgent* $(BIN_DIR)
	/bin/chmod +x $(BIN_DIR)/*
endif



setup:
	/bin/mkdir -p $(SHARE_DIR)
	/bin/mkdir -p $(LIB_DIR)
	/bin/mkdir -p $(BIN_DIR)


install: setup build

all: setup build


clean:
	/bin/rm -rf $(LIB_DIR)/*

