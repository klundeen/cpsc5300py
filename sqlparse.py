""" minimal SQL parser

By: Kevin Lundeen
For: CPSC 4300/5300, S17

Based on http://pyparsing.wikispaces.com/file/view/simpleSQL.py: Copyright (c) 2003,2016, Paul McGuire
"""
from pyparsing import CaselessLiteral, Dict, Word, delimitedList, Optional, \
    Combine, Group, alphas, nums, alphanums, Forward, oneOf, quotedString, \
    ZeroOrMore, restOfLine, CaselessKeyword, ParseResults

# define SQL keywords
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
                  'WHERE','WIDTH_BUCKET','WINDOW','WITH','WITHIN','WITHOUT','YEAR'}
AND = CaselessKeyword("AND")
CREATE = CaselessKeyword("CREATE")
DOUBLE = CaselessKeyword("DOUBLE")
FROM = CaselessKeyword("FROM")
IN = CaselessKeyword("IN")
INT = CaselessKeyword("INT") | CaselessKeyword("INTEGER")
OR = CaselessKeyword("OR")
SELECT = CaselessKeyword("SELECT")
TABLE = CaselessKeyword("TABLE")
TEXT = CaselessKeyword("TEXT")
VARCHAR = CaselessKeyword("VARCHAR")
WHERE = CaselessKeyword("WHERE")

selectStmt = Forward()
whereExpression = Forward()

ident = Word(alphas, alphanums + "_$").setName("identifier")
datatype = Group(VARCHAR + "(" + Word(nums) + ")") | INT | TEXT | DOUBLE
columnName = (delimitedList(ident, ".", combine=True))
columnNameList = Group(delimitedList(columnName))
column_definition = Group(ident("column_name") + datatype("data_type"))
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

columnRval = realNum | intNum | quotedString | columnName  # need to add support for alg expressions
whereCondition = Group(
    (columnName + binop + columnRval) |
    (columnName + IN + "(" + delimitedList(columnRval) + ")") |
    (columnName + IN + "(" + selectStmt + ")") |
    ("(" + whereExpression + ")")
)
whereExpression << whereCondition + ZeroOrMore((AND | OR) + whereExpression)

# top level statements
table_definition = CREATE + TABLE + table_name("table_name") + "(" + column_definition_list("table_element_list") + ")"
selectStmt <<= (SELECT + ('*' | columnNameList)("columns") +
                FROM + table_names("table_names") +
                Optional(Group(WHERE + whereExpression), "")("where"))

SQLstatement = table_definition("table_definition") | selectStmt("query")

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
        Select A,b from table1,table2 where table1.id eq table2.id""")
