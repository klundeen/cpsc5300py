""" minimal SQL parser

By: Kevin Lundeen
For: CPSC 4300/5300, S17

Based on http://pyparsing.wikispaces.com/file/view/simpleSQL.py: Copyright (c) 2003,2016, Paul McGuire

Using grammar non-terminal names from sql2003 where possible.
"""
from pyparsing import CaselessLiteral, Dict, Word, delimitedList, Optional, \
    Combine, Group, nums, alphanums, Forward, oneOf, quotedString, \
    ZeroOrMore, restOfLine, CaselessKeyword

# define SQL keywords
NON_STANDARD_RESERVED_WORDS = {'BTREE', 'COLUMNS', 'HASH', 'INDEX', 'SHOW', 'TABLES'}
RESERVED_WORDS = {'ADD','ALL','ALLOCATE','ALTER','AND','ANY','ARE','ARRAY','AS','ASENSITIVE','ASYMMETRIC','AT',
                  'ATOMIC','AUTHORIZATION','BEGIN','BETWEEN','BIGINT','BINARY','BLOB','BOOLEAN','BOTH','BY','CALL',
                  'CALLED','CASCADED','CASE','CAST','CHAR','CHARACTER','CHECK','CLOB','CLOSE','COLLATE','COLUMN',
                  'COMMIT','CONNECT','CONSTRAINT','CONTINUE','CORRESPONDING','CREATE','CROSS','CUBE','CURRENT',
                  'CURRENT_DATE','CURRENT_DEFAULT_TRANSFORM_GROUP','CURRENT_PATH','CURRENT_ROLE','CURRENT_TIME',
                  'CURRENT_TIMESTAMP','CURRENT_TRANSFORM_GROUP_FOR_TYPE','CURRENT_USER','CURSOR','CYCLE','DATE',
                  'DAY','DEALLOCATE','DEC','DECIMAL','DECLARE','DEFAULT','DELETE','DEREF','DESCRIBE','DETERMINISTIC',
                  'DISCONNECT','DISTINCT','DOUBLE','DROP','DYNAMIC','EACH','ELEMENT','ELSE','END','END-EXEC','ESCAPE',
                  'EXCEPT','EXEC','EXECUTE','EXISTS','EXTERNAL','FALSE','FETCH','FILTER','FLOAT','FOR','FOREIGN',
                  'FREE','FROM','FULL','FUNCTION','GET','GLOBAL','GRANT','GROUP','GROUPING','HAVING','HOLD','HOUR',
                  'IDENTITY','IMMEDIATE','IN','INDICATOR','INNER','INOUT','INPUT','INSENSITIVE','INSERT','INT',
                  'INTEGER','INTERSECT','INTERVAL','INTO','IS','ISOLATION','JOIN','LANGUAGE','LARGE','LATERAL',
                  'LEADING','LEFT','LIKE','LOCAL','LOCALTIME','LOCALTIMESTAMP','MATCH','MEMBER','MERGE','METHOD',
                  'MINUTE','MODIFIES','MODULE','MONTH','MULTISET','NATIONAL','NATURAL','NCHAR','NCLOB','NEW','NO',
                  'NONE','NOT','NULL','NUMERIC','OF','OLD','ON','ONLY','OPEN','OR','ORDER','OUT','OUTER','OUTPUT',
                  'OVER','OVERLAPS','PARAMETER','PARTITION','PRECISION','PREPARE','PRIMARY','PROCEDURE','RANGE',
                  'READS','REAL','RECURSIVE','REF','REFERENCES','REFERENCING','REGR_AVGX','REGR_AVGY','REGR_COUNT',
                  'REGR_INTERCEPT','REGR_R2','REGR_SLOPE','REGR_SXX','REGR_SXY','REGR_SYY','RELEASE','RESULT','RETURN',
                  'RETURNS','REVOKE','RIGHT','ROLLBACK','ROLLUP','ROW','ROWS','SAVEPOINT','SCROLL','SEARCH','SECOND',
                  'SELECT','SENSITIVE','SESSION_USER','SET','SIMILAR','SMALLINT','SOME','SPECIFIC','SPECIFICTYPE',
                  'SQL','SQLEXCEPTION','SQLSTATE','SQLWARNING','START','STATIC','SUBMULTISET','SYMMETRIC','SYSTEM',
                  'SYSTEM_USER','TABLE','THEN','TIME','TIMESTAMP','TIMEZONE_HOUR','TIMEZONE_MINUTE','TO','TRAILING',
                  'TRANSLATION','TREAT','TRIGGER','TRUE','UESCAPE','UNION','UNIQUE','UNKNOWN','UNNEST','UPDATE',
                  'UPPER','USER','USING','VALUE','VALUES','VAR_POP','VAR_SAMP','VARCHAR','VARYING','WHEN','WHENEVER',
                  'WHERE','WIDTH_BUCKET','WINDOW','WITH','WITHIN','WITHOUT','YEAR'} | NON_STANDARD_RESERVED_WORDS
AND = CaselessKeyword("AND")
BOOLEAN = CaselessKeyword("BOOLEAN")
BTREE = CaselessKeyword("BTREE")
COLUMNS = CaselessKeyword("COLUMNS")
CREATE = CaselessKeyword("CREATE")
DELETE = CaselessKeyword("DELETE")
DOUBLE = CaselessKeyword("DOUBLE")
DROP = CaselessKeyword("DROP")
FROM = CaselessKeyword("FROM")
HASH = CaselessKeyword("HASH")
IN = CaselessKeyword("IN")
INDEX = CaselessKeyword("INDEX")
INSERT = CaselessKeyword("INSERT")
INT = CaselessKeyword("INT") | CaselessKeyword("INTEGER")
INTO = CaselessKeyword("INTO")
JOIN = CaselessKeyword("JOIN")
KEY = CaselessKeyword("KEY")
ON = CaselessKeyword("ON")
OR = CaselessKeyword("OR")
PRIMARY = CaselessKeyword("PRIMARY")
SELECT = CaselessKeyword("SELECT")
SHOW = CaselessKeyword("SHOW")
TABLE = CaselessKeyword("TABLE")
TABLES = CaselessKeyword("TABLES")
TEXT = CaselessKeyword("TEXT")
UNIQUE = CaselessKeyword("UNIQUE")
USING = CaselessKeyword("USING")
VALUES = CaselessKeyword("VALUES")
VARCHAR = CaselessKeyword("VARCHAR")
WHERE = CaselessKeyword("WHERE")

query = Forward()
whereExpression = Forward()

ident = Word(alphanums + "_$").setName("identifier")
data_type = Group(VARCHAR + "(" + Word(nums) + ")") | INT | TEXT | DOUBLE | BOOLEAN
column_name = (delimitedList(ident, ".", combine=True)("column_name"))
column_name_list = Group(delimitedList(column_name))
primary_key = PRIMARY + KEY + "(" + column_name_list("key_columns") + ")"
column_definition = Group(ident("def_column_name") + data_type("data_type")) | Group(primary_key("primary_key"))
column_definition_list = Dict(delimitedList(column_definition))
table_name = delimitedList(ident, ".", combine=True)
table_names = Group(delimitedList(table_name))

E = CaselessLiteral("E")
binop = oneOf("= != < > >= <= eq ne lt le gt ge", caseless=True)
arithSign = Word("+-", exact=1)
realNum = Combine(Optional(arithSign) + (Word(nums) + "." + Optional(Word(nums)) |
                                         ("." + Word(nums))) +
                  Optional(E + Optional(arithSign) + Word(nums)))
intNum = Combine(Optional(arithSign) + Word(nums) +
                 Optional(E + Optional("+") + Word(nums)))

columnRval = realNum | intNum | quotedString | column_name  # need to add support for alg expressions
whereCondition = Group(
    (column_name + binop + columnRval) |
    (column_name + IN + "(" + delimitedList(columnRval) + ")") |
    (column_name + IN + "(" + query + ")") |
    ("(" + whereExpression + ")")
)
whereExpression << whereCondition + ZeroOrMore((AND | OR) + whereExpression)
value = realNum | intNum | quotedString
value_list = Group(delimitedList(value))

join_using = Group(JOIN + table_name("join_table") + USING + "(" + column_name_list("join_columns") + ")")

# top level statements
table_definition = CREATE + TABLE + table_name("table_name") + "(" + column_definition_list("table_element_list") + ")"
index_definition = (CREATE + Optional(UNIQUE)("unique") + INDEX + ident("index_name") + ON +
                    table_name("table_name") + "(" + column_name_list("columns") + ")" +
                    Optional(USING + (BTREE | HASH)("index_type")))
drop_table_statement = DROP + TABLE + table_name("table_name")
drop_index_statement = DROP + INDEX + ident("index_name") + ON + table_name("table_name")
show_tables_statement = SHOW + TABLES
show_columns_statement = SHOW + COLUMNS + FROM + table_name("table_name")
show_index_statement = SHOW + INDEX + FROM + table_name("table_name")
insert_statement = (INSERT + INTO + table_name("table_name") + Optional("(" + column_name_list("columns") + ")") +
                    VALUES + "(" + value_list("values") + ")")
delete_statement = DELETE + FROM + table_name("table_name") + Optional(Group(WHERE + whereExpression), "")("where")
query <<= (SELECT + ('*' | column_name_list)("columns") +
                FROM + table_names("table_names") +
                ZeroOrMore(join_using)("joins") +
                Optional(Group(WHERE + whereExpression), "")("where"))

SQLstatement = (table_definition("table_definition") |
                index_definition("index_definition") |
                insert_statement("insert_statement") |
                delete_statement("delete_statement") |
                query("query") |
                drop_table_statement("drop_table_statement") |
                drop_index_statement("drop_index_statement") |
                show_tables_statement("show_tables_statement") |
                show_columns_statement("show_columns_statement") |
                show_index_statement("show_index_statement"))

# define Oracle comment format, and ignore them
oracleSqlComment = "--" + restOfLine
SQLstatement.ignore(oracleSqlComment)


if __name__ == "__main__":
    SQLstatement.runTests("""\

        # create
        CREATE TABLE fux (fruit VARCHAR(32), veggie INT)

        # multiple tables
        SELECT * from XYZZY, ABC

        # dotted table name
        select * from SYS.XYZZY

        Select A from Sys.dual

        Select A,B,C from Sys.dual

        Select A, B, C from Sys.dual, Table2

        # FAIL - invalid SELECT keyword
        Xelect A, B, C from Sys.dual

        # FAIL - invalid FROM keyword
        Select A, B, C frox Sys.dual

        # FAIL - incomplete statement
        Select

        # FAIL - incomplete statement
        Select * from

        # FAIL - invalid column
        Select &&& frox Sys.dual

        # where clause
        Select A from Sys.dual where a in ('RED','GREEN','BLUE')

        # compound where clause
        Select A from Sys.dual where a in ('RED','GREEN','BLUE') and b in (10,20,30)

        # where clause with comparison operator
        Select A,b from table1,table2 where table1.id eq table2.id
        
        # join
        SELECT * FROM r JOIN s USING(x,y) JOIN t USING(z) JOIN u USING(w) WHERE a < b""")
