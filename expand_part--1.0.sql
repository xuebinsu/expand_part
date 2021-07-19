CREATE OR REPLACE FUNCTION expand_partitioned_table_prepare(partitioned_table regclass)
RETURNS INT STRICT
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION expand_partitioned_table_redistribute_part(partitioned_table regclass, part regclass)
RETURNS INT STRICT
AS 'MODULE_PATHNAME'
LANGUAGE C;