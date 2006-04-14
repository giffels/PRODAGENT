#!/usr/bin/make

PYTHON_SRC=$(shell pwd)/src/python
PYTHON_LIB=$(shell pwd)/lib

all:
	cd $(PYTHON_SRC); python setup.py build --build-lib=$(PYTHON_LIB)
	/bin/cp $(PYTHON_SRC)/ShREEK/shreek $(PYTHON_LIB)/ShREEK


clean:
	/bin/rm -rf $(PYTHON_LIB)/*
