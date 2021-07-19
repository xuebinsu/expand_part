MODULE_big = expand_part

EXTENSION = $(MODULE_big)
DATA = $(MODULE_big)--1.0.sql
SRCDIR = ./
FILES = $(shell find $(SRCDIR) -type f -name "*.c")
OBJS = $(foreach FILE,$(FILES),$(subst .c,.o,$(FILE)))

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

.PHONY: rebuild
rebuild:
	make install
	psql -c 'drop extension if exists expand_part;'
	psql -c 'create extension expand_part;'
