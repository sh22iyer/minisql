%{
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/******** FORWARD DECLARATION FOR STUFF NEEDED BY THE LEXER ********/

/* Currently our symbol table is a just a array of strings. 
 * Identifiers, strings constants, integer constant are just "inserted"
 * into s_id_names[] using s_id_index as the index. In a real parser,
 * the symbol table will be a hash map with more metadata.
 */
#define MAX_ID_LEN		64
#define MAX_NUM_IDS		100
static int  s_id_index = 0;
static char s_id_names[MAX_NUM_IDS][MAX_ID_LEN];

/* addXXXX() prototypes for building the parse tree */
void addTableNameNode(char *id);
void addSelectAllColumnsNode();
void addSelectColumnNameNode(char *id);
void addGroupByColumnExprNode(char *id, int fcode);
void addSelectConditionNode(char *id, int comparator, int id2);
void addGroupByColumnNode(char *id);
void addOrderByColumnNode(char *id);

#define SUCCESS					1
#define FAILED					0

#define TRUE					1
#define FALSE					0

%}
%token SELECT
%token FROM
%token WHERE
%token ORDER
%token GROUP
%token BY
%token SUM
%token COUNT
%token IDENTIFIER
%token INTEGER
%token STRINGLITERAL
%token EQ
%token GT
%token LT
%token GE
%token LE
%token NE
%token OB
%token CB
%token COMMA
%token ASTERISK
%token HAVING
%%

/* SQL Select statement (SFW => Select-From-Where)
 * The where_clause, groupby_cause, orderby_clause are optional.
 * Currently a single table name supported, need to be extended for JOINs.
 * The parse tree is built up as each non-terminal is recognized by
 * calling the addXXXX() functions.
 * The parse tree root is the global variable "s_select_stmt"
 */
select_stmt : SELECT select_list FROM IDENTIFIER where_clause groupby_clause orderby_clause
		{ addTableNameNode(s_id_names[$4]); }


/* select columns - can be a single "*" or a list of column names & expressions */
select_list : ASTERISK
		{ addSelectAllColumnsNode(); }
	| select_col_list


/* list of columns/expressions : check column_expre below */
select_col_list : column_expr COMMA select_col_list
	| column_expr


/* A column in a select list can be :-
 *    Identifier : only column name for now, can be SQL functions etc
 *    group by aggregate functions : sum(<column name>) and count(*) and
 *                                   count(<column name>) for now
 */
column_expr : IDENTIFIER
		{ addSelectColumnNameNode(s_id_names[$1]); }
	| SUM OB IDENTIFIER CB
		{ addGroupByColumnExprNode(s_id_names[$3], SUM); }
	| COUNT OB ASTERISK CB
		{ addGroupByColumnExprNode("*", COUNT); }
	| COUNT OB IDENTIFIER CB
		{ addGroupByColumnExprNode("*", COUNT); } /* column name not important */


/* Support single comparision expression for SFW. Does not support
 * AND or OR logical operators
 */
where_clause :
	| WHERE simple_expr


/* simple comparision expression, only supports :-
 *    column = rhs value
 *    column != rhs value
 *    column > rhs value
 */
simple_expr : IDENTIFIER EQ term_expr
		{ addSelectConditionNode(s_id_names[$1], EQ, $3); }
	| IDENTIFIER GT term_expr
		{ addSelectConditionNode(s_id_names[$1], GT, $3); }
	| IDENTIFIER NE term_expr
		{ addSelectConditionNode(s_id_names[$1], NE, $3); }


/* right hand side of comparision expression, right now can only
 * be string literal or an integer
 */
term_expr : STRINGLITERAL
		{ $$ = $1; }
	| INTEGER
		{ sprintf(s_id_names[s_id_index], "%d", $1);
		  $$ = s_id_index; s_id_index++; }


/* group by <column name> - support only single column name */
groupby_clause :
	| GROUP BY IDENTIFIER
		{ addGroupByColumnNode(s_id_names[$3]); }


/* order by <column name>  - single only right now
 * order by <number> where number is column number in select list
 */
orderby_clause :
	| ORDER BY IDENTIFIER
		{ addOrderByColumnNode(s_id_names[$3]); }
	| ORDER BY INTEGER
		{ sprintf(s_id_names[s_id_index], "%d", $3);
		  addOrderByColumnNode(s_id_names[s_id_index]);
                  s_id_index++;};
%%

/* Various Node types corresponding to nodes in the SQL parse tree */

/* TableNameNode		- single Table name in the SFW statement
 * SelectColumnListNode		- list of columns
 * AggregateColumnExpreNode	- aggregate function expr in select list
 * SelectConditionNode		- the WHERE clause filter condition
 * GroupByColumnNode		- column name in group by (single)
 * OrderByColumnNode		- column name or number in order by clause
 *
 * SelectStmtNode		- The parent Select statement node, other
 *				  nodes are children.
 */
typedef struct
{
  char tableName[MAX_ID_LEN];
} TableNameNode;

typedef struct SelectColumnListNode
{
  char                         columnName[MAX_ID_LEN];
  struct SelectColumnListNode  *nextColumn; /* list of columns in select */
} SelectColumnListNode;

typedef struct 
{
  char columnName[MAX_ID_LEN];
  int  fcode; /* SUM or MAX or AVG etc */
} AggregateColumnExprNode;

typedef struct
{
  char columnName[MAX_ID_LEN];
  int  comparator; /* = or > or < etc */
  char operand[MAX_ID_LEN]; /* for now */
} SelectConditionNode;

typedef struct
{
  char columnName[MAX_ID_LEN];
} GroupByColumnNode;

typedef struct
{
  char columnName[MAX_ID_LEN];
} OrderByColumnNode;

typedef struct 
{
  TableNameNode            *selectTableName;
  SelectColumnListNode     *selectColumnList;

  /* next 4 are optional */
  AggregateColumnExprNode  *aggColumnExpr;
  SelectConditionNode      *selectCondition;
  GroupByColumnNode        *groupBy;
  OrderByColumnNode        *orderBy;
} SelectStmtNode; /* the complete select statement */

/**** GLOBALS *****/
SelectStmtNode      *s_stmt; /* top level for SELECT statement parse tree */

/* The addXXX() functions add respective nodes to the SQL statement parse tree */
/*******************************************************************************/

void addTableNameNode(char *id)
{
  s_stmt->selectTableName = (TableNameNode *)malloc(sizeof(TableNameNode));
  strcpy(s_stmt->selectTableName->tableName, id);
}

void addSelectAllColumnsNode()
{
  addSelectColumnNameNode("*");
}

void addSelectColumnNameNode(char *id)
{
  SelectColumnListNode *cnode = (SelectColumnListNode *)malloc(sizeof(SelectColumnListNode));
  strcpy(cnode->columnName, id);
  cnode->nextColumn = 0;

  if (!s_stmt->selectColumnList)
  {
    s_stmt->selectColumnList = cnode;
  }
  else
  { /* traverse to end and append */
    SelectColumnListNode *s = s_stmt->selectColumnList, *p;

    while (s)
    {
      p = s;
      s = s->nextColumn;
    }
 
    p->nextColumn = cnode; /* append */
  }
}

void addGroupByColumnExprNode(char *id, int fcode)
{
  s_stmt->aggColumnExpr = 
    (AggregateColumnExprNode *)malloc(sizeof(AggregateColumnExprNode));
  strcpy(s_stmt->aggColumnExpr->columnName, id);
  s_stmt->aggColumnExpr->fcode = fcode; /* SUM/MAX/AVG etc */
}

void addSelectConditionNode(char *id, int comparator, int id2)
{
  s_stmt->selectCondition = 
    (SelectConditionNode *)malloc(sizeof(SelectConditionNode));
  strcpy(s_stmt->selectCondition->columnName, id);
  s_stmt->selectCondition->comparator = comparator;
  strcpy(s_stmt->selectCondition->operand, s_id_names[id2]);
}

void addOrderByColumnNode(char *id)
{
  s_stmt->orderBy = (OrderByColumnNode *)malloc(sizeof(OrderByColumnNode));
  strcpy(s_stmt->orderBy->columnName, id);
}

/*******************************************************************************/

void addGroupByColumnNode(char *id)
{
  s_stmt->groupBy = (GroupByColumnNode *)malloc(sizeof(GroupByColumnNode));
  strcpy(s_stmt->groupBy->columnName, id);
}

/* printStmtParseTree : print the SQL statement parse tree. Distinctly
 * prints the various Nodes of the parse tree. This is a good visual
 * guide to how the SQL statement has been broken into constituents
 */
void printStmtParseTree(SelectStmtNode *s)
{
  printf("------------------- Statement Parse Tree -------------------\n");

  printf ("SELECT stmt -->\t");

  printf ("<select column list> => ");
  { 
    SelectColumnListNode *p = s->selectColumnList;
    while (p) { printf("%s ", p->columnName); p = p->nextColumn;}
    printf(" .\t");
  }

  printf ("<table> => %s\t", s->selectTableName->tableName);
 
  if (!s->selectCondition)
  {
    printf("<filter> => (empty)\t");
  }
  else
  {
    printf("<filter> => %s %d(op) %s\t", s->selectCondition->columnName,
           s->selectCondition->comparator, s->selectCondition->operand);
  }

  if (!s->groupBy)
  {
    printf("<group by> => (empty)\t");
  }
  else
  {
    char *f = (s->aggColumnExpr->fcode == SUM) ? "sum" :  "count";
    printf("<group by> => column = %s,", s->groupBy->columnName);
    printf("aggregate function = %s, column = %s\t",
              f, s->aggColumnExpr->columnName);
  }

  if (!s->orderBy)
  {
    printf("<order by> => (empty)\t");
  }
  else
  {
    printf("<order by> => column = %s\t", s->orderBy->columnName);
  }

  printf("\n\n");
}

/* The Metadata for this SQL parser. There are 2 primary structures :-
 * TableDef  : Table definition with list of columns etc
 * IndexDef  : Index defintion with column name, index type
 * and
 * ColumnDef : A single column definition inside a TableDef
 *
 * The Metadata is "built up" by calling the addColumn/addIndex functions.
 */

/********************** IndexDef ***********************/
#define ITYPE_BTREE		1
#define ITYPE_HASH		2
#define ITYPE_BITMAP		3

/* Does the index provide sorted order access? Important for range
 * scans and ordering optimization.
 */
#define SORTED_ACCESS		1
#define NOT_SORTED		0

/* Unique index or duplicate keys allowed? */
#define UNIQUE			1
#define NOT_UNIQUE		0

/* Index can be used for range scan */
#define RANGE_SCAN		1
#define NO_RANGE_SCAN		0

typedef struct
{
  char  iName[MAX_ID_LEN];
  char  cName[MAX_ID_LEN]; /* table column name */
  int   iType;             /* BTREE, HASH, BITMAP etc */
  int   iSorted;           /* sorted access? 1 = BTREE, 0 for most others */
  int   iUnique;
  int   iRangeAccess;      /* 1 - supports range scan, 0 - does not */
} IndexDef;

/****************** ColumnDef *************************/
#define CTYPE_NUMBER		1
#define CTYPE_VARCHAR		2
typedef struct
{
  char  cName[MAX_ID_LEN];
  int   cType;
  int   cLength; /* for strings etc */
} ColumnDef;


/************************* TableDef **************************/
#define MAX_COLUMNS		10
#define MAX_INDEXES		10


typedef struct
{
  char            tName[MAX_ID_LEN];
  int             tColumnsCount;
  ColumnDef       tColumns[MAX_COLUMNS];
  int             tIndexCount;
  IndexDef        tIndexes[MAX_INDEXES];

  int             tIsSharded; /* true/false */
  char            tShardColumn[MAX_ID_LEN];
} TableDef;


/* add a single Column */
void addColumn(TableDef *tableDef, char *column, int datatype, int clen)
{
  int i = tableDef->tColumnsCount;

  strcpy(tableDef->tColumns[i].cName, column);
  tableDef->tColumns[i].cType = datatype;
  tableDef->tColumns[i].cLength = clen;

  tableDef->tColumnsCount++;
}

/* add/define a Index */
void addIndex(TableDef *tableDef, char *indexname, char *column, int itype, 
              int sorted, int unique, int rangescan)
{
  int i = tableDef->tIndexCount;

  strcpy(tableDef->tIndexes[i].iName, indexname);
  strcpy(tableDef->tIndexes[i].cName, column);

  tableDef->tIndexes[i].iType        = itype;
  tableDef->tIndexes[i].iSorted      = sorted;
  tableDef->tIndexes[i].iUnique      = unique;
  tableDef->tIndexes[i].iRangeAccess = rangescan;

  tableDef->tIndexCount++;
}

/* shard or partition a table - by the specified column */
void setShardingKey(TableDef *tableDef, char *column)
{
  tableDef->tIsSharded = 1;
  strcpy(tableDef->tShardColumn, column);
}

/* The Data Model for this SQL parser contains 2 tables :-
 * 1. saleshistory
 * 2. customers
 *
 * Table definitions for both are stored in global variables.
 * Below defineSalesHistoryT() and defineCustomersT() show how to add new
 * tables for our parser!
 */
TableDef *salesHistoryT = 0;
TableDef *customersT    = 0;

#define SALESHISTORY			"saleshistory"
#define CUSTOMERS			"customers"

void defineSalesHistoryT()
{
  salesHistoryT = (TableDef *)malloc(sizeof(TableDef));
  memset(salesHistoryT, 0, sizeof(TableDef));

  strcpy(salesHistoryT->tName, SALESHISTORY);

  addColumn(salesHistoryT, "product_id", CTYPE_VARCHAR, 30);
  addColumn(salesHistoryT, "cust_id", CTYPE_VARCHAR, 30);
  addColumn(salesHistoryT, "sales", CTYPE_NUMBER, 12); /* sale amount */
  addColumn(salesHistoryT, "qrcode", CTYPE_VARCHAR, 128);/* test */

  /* a few index to check our query optimizer! */
  addIndex(salesHistoryT, "saleshistory$products", "product_id",
           ITYPE_BTREE, SORTED_ACCESS, NOT_UNIQUE, RANGE_SCAN);
  addIndex(salesHistoryT, "saleshistory$customers", "cust_id",
           ITYPE_BTREE, SORTED_ACCESS, NOT_UNIQUE, RANGE_SCAN);
  addIndex(salesHistoryT, "saleshistory$qrcode", "qrcode",
           ITYPE_HASH, NOT_SORTED, UNIQUE, NO_RANGE_SCAN);
}

void defineCustomersT()
{
  customersT = (TableDef *)malloc(sizeof(TableDef));
  memset(customersT, 0, sizeof(TableDef));

  strcpy(customersT->tName, CUSTOMERS);
  addColumn(customersT, "cust_id",   CTYPE_VARCHAR, 30);
  addColumn(customersT, "cust_name", CTYPE_VARCHAR, 100);
  addColumn(customersT, "zipcode",   CTYPE_VARCHAR, 10);
  addColumn(customersT, "category",  CTYPE_VARCHAR, 4);

  addIndex(customersT, "customers$id", "cust_id", 
           ITYPE_BTREE, SORTED_ACCESS, UNIQUE, RANGE_SCAN);
  addIndex(customersT, "customers$category", "category",
           ITYPE_BITMAP, NOT_SORTED, NOT_UNIQUE, NO_RANGE_SCAN);
}

void LoadMetadata()
{
  /* add the 2 tables, pretty static now! */
  defineSalesHistoryT();
  defineCustomersT();
}

/* the initial query plan has a fixed precedence */
/* Step 1 - read table
 * Step 2 - apply where clause filter
 * Step 3 - any group by
 * Step 4 - select the columns
 * Step 5 - any order by
 * 
 * #1 and #4 are always present in a SELECT ... FROM <table> SQL statement
 */

#define OP_READTABLE			1
#define OP_SELECTFILTER			2
#define OP_GROUPING			3
#define OP_PROJECTCOLUMNS		4
#define OP_ORDERING			5

typedef struct LogicalOperation
{
  int    opcode;
  char   outputstr[MAX_ID_LEN]; /* for now just descriptive string */
  void*  associatedParseNode;

  struct LogicalOperation *nextOperation; /* for list */
} LogicalOperation;

void initialQueryPlan(SelectStmtNode *s)
{
  LogicalOperation *op, *plan = 0;
  int i, level = 0;

  op = (LogicalOperation *)malloc(sizeof(LogicalOperation));
  op->opcode = OP_READTABLE;
  sprintf(op->outputstr, "Read table %s", s->selectTableName->tableName);
  op->associatedParseNode = s->selectTableName;
  op->nextOperation = 0;

  /* build the complete plan */
  plan = op;

  /* next */
  if (s->selectCondition)
  {
    op = (LogicalOperation *)malloc(sizeof(LogicalOperation));
    op->opcode = OP_SELECTFILTER;
    sprintf(op->outputstr, "Apply select filter");
    op->associatedParseNode = s->selectCondition;
    op->nextOperation = 0;
    op->nextOperation = plan;
    plan = op;
  }

  if (s->groupBy)
  {
    op = (LogicalOperation *)malloc(sizeof(LogicalOperation));
    op->opcode = OP_GROUPING;
    sprintf(op->outputstr, "Grouping");
    op->associatedParseNode = s->groupBy;
    op->nextOperation = 0;
    op->nextOperation = plan;
    plan = op;
  }

  op = (LogicalOperation *)malloc(sizeof(LogicalOperation));
  op->opcode = OP_PROJECTCOLUMNS;
  sprintf(op->outputstr, "Project columns");
  op->associatedParseNode = s->selectColumnList;
  op->nextOperation = 0;

  op->nextOperation = plan;
  plan = op;

  if (s->orderBy)
  {
    op = (LogicalOperation *)malloc(sizeof(LogicalOperation));
    op->opcode = OP_ORDERING;
    sprintf(op->outputstr, "Ordering");
    op->associatedParseNode = s->orderBy;
    op->nextOperation = 0;

    op->nextOperation = plan;
    plan = op;
  }

  /* print the initial query plan */
  op = plan;
  printf("-------------------Basic Query Plan------------------\n");
  while (op)
  {
    for (i = 0; i < level; i++) printf("\t");

    printf("%s\n",op->outputstr);
    op = op->nextOperation;

    level++;
  }
  
}

/* semanticAnalysis - very basic semantic analysis right now! */
int semanticAnalysis(SelectStmtNode *s)
{
  if (strcmp(s->selectTableName->tableName, SALESHISTORY) &&
      strcmp(s->selectTableName->tableName, CUSTOMERS))
  {
    yyerror("bad table name\n");
    return FAILED;
  }
  return SUCCESS;
}

/* getIndex - return index (if defined) on the column */
IndexDef* getIndex(TableDef *t, char *columnname)
{
  int i;

  for (i = 0; i < t->tIndexCount; i++)
  {
    if (!strcmp(t->tIndexes[i].cName, columnname))
      return &(t->tIndexes[i]); /* found */
  }
  return (IndexDef *)0;
}

/**********************buildOptimalQueryPLan***************************/
/* This is a pure rule-based optimizer as we do not have statistics or
 * past performance history. This routine forms the query plan using a
 * well established set of rules as commented below.
 */

/* ExecOperationNode - Physical operation step in the query execution plan */
#define OP_TABLE_SCAN				1
#define OP_TABLE_FETCH				2 /* fetch rows via index */
#define OP_INDEX_FETCH				3
#define OP_INDEX_RANGE_FETCH			4
#define OP_HASH_GROUPBY				6
#define OP_SORT_GROUPBY				7
#define OP_INDEX_SORT_GROUPBY			8
#define OP_SORT_ORDERING		        9

typedef struct ExecOperationNode
{
  int         opcode;
  char        opcodeDescription[MAX_ID_LEN];
  void        *extraInfo;

  struct ExecOperationNode *nextOp;
} ExecOperationNode;

void printFinalPlan(ExecOperationNode *plan)
{
  ExecOperationNode *p = plan;
  int level = 0, i = 0;

  printf("\n--------------Final Query Execution Plan-------------\n\n");

  while (p)
  {
    for (i = 0; i < level; i++) printf("\t"); /* indent */
    printf("%s\n",p->opcodeDescription);
    p = p->nextOp;
    level++;
  }
}

void buildOptimalQueryPlan(SelectStmtNode *s)
{
  ExecOperationNode *plan = 0;
  ExecOperationNode *indexOp = 0, *tablescanOp = 0;
  ExecOperationNode *groupByOp = 0;
  ExecOperationNode *sortingOp = 0;

  int indexOnlyOp = FALSE;

  /* we will work directly on the parse tree */
  TableDef *td;

  if (!strcmp(s->selectTableName->tableName, SALESHISTORY))
    td = salesHistoryT;
  else
    td = customersT;
  
  /* Step 1 - check if index can be used for filter and FTS avoided */
  if (s->selectCondition)
  {
    IndexDef *index = getIndex(td, s->selectCondition->columnName);
    int useIndex = FALSE, rangescan = FALSE;
    if (index)
    {
      /* we will not use index for not-equals. For other range conditions,
       * statistics based selectivity ratio via zipfian distribution etc
       * must be used to decide if index will be beneficial. And some indexes
       * do not support range queries (e.g a HASH index cannot be use for
       * evaluating (id > 2018192100233)
       */
       if (s->selectCondition->comparator == NE)
         useIndex = FALSE;
       else if (s->selectCondition->comparator == GT)
       {
         /* check if underlying index supports range queries */
         if (index->iRangeAccess)
         {
           useIndex = TRUE;
           rangescan = TRUE;
         }
         else
           useIndex = FALSE;
       }
       else /* is EQ comparision */
       {
         useIndex = TRUE;
         rangescan = FALSE;
       }
    }

    if (useIndex)
    {
      indexOp            = (ExecOperationNode *)malloc(sizeof(ExecOperationNode));
      indexOp->opcode    =  (!rangescan) ? OP_INDEX_FETCH : OP_INDEX_RANGE_FETCH;
      indexOp->extraInfo = (void *)index;
      indexOp->nextOp    = 0;

      if (indexOp->opcode == OP_INDEX_FETCH)
        sprintf(indexOp->opcodeDescription, "index fetch %s", index->iName);
      else
        sprintf(indexOp->opcodeDescription, "index range fetch %s", index->iName);

      indexOnlyOp = TRUE; /* can change below */
    }
  }

  /* Step 2 - can we do a index only scan ? */
  /* check the columns in the select list and any aggregation expr */
  if (indexOp)
  {
    SelectColumnListNode *p = s->selectColumnList;
    while (p)
    {
      if (strcmp(p->columnName, ((IndexDef *)indexOp->extraInfo)->cName))
      {
        indexOnlyOp = FALSE; /* found different column */
        break;
      }
    }
  }

  if (!indexOp || !indexOnlyOp)
  {
    tablescanOp = (ExecOperationNode *)malloc(sizeof(ExecOperationNode));
    tablescanOp->extraInfo = (void *)td;
    tablescanOp->nextOp    = 0;

    if (indexOp)
    {
      tablescanOp->opcode = OP_TABLE_FETCH;
      sprintf(tablescanOp->opcodeDescription, "table fetch %s", td->tName);

      tablescanOp->nextOp = indexOp; plan = tablescanOp;
    }
    else
    { /* FTS !! */
      tablescanOp->opcode = OP_TABLE_SCAN;
      sprintf(tablescanOp->opcodeDescription, "table scan full %s", td->tName);

      plan = tablescanOp;
    }
  } /* !indexOp */

  /* if grouping needed, select grouping method (HASH or SORT) */
  if (s->groupBy)
  {
    int indexonly = FALSE;
    groupByOp = (ExecOperationNode *)malloc(sizeof(ExecOperationNode));
    groupByOp->extraInfo = (void *)s->groupBy;
    groupByOp->nextOp    = 0;

    /* check if group by column and index column are same and sorted access */
    if (indexOp)
    {
      IndexDef *id = (IndexDef *)indexOp->extraInfo;
      if (!strcmp(s->groupBy->columnName, id->cName) &&
          id->iSorted)
      {
        groupByOp->opcode = OP_INDEX_SORT_GROUPBY;
        sprintf(groupByOp->opcodeDescription,"(group via index sort)");
        indexonly = TRUE;
      }

    }
    /* decide between hash group or sorted group */
    if (!indexonly)
    {
      if (!s->orderBy || 
             (strcmp(s->orderBy->columnName, s->groupBy->columnName)))
      {
        groupByOp->opcode = OP_HASH_GROUPBY;
        sprintf(groupByOp->opcodeDescription,"hash group by");
      }
      else
      {
        groupByOp->opcode = OP_SORT_GROUPBY;
        sprintf(groupByOp->opcodeDescription,"sort group by");
      }
    }

    groupByOp->nextOp = plan;
    plan = groupByOp;
  } /* if groupBy */

  /* any sort for ordering ? */
  if (s->orderBy)
  {
    if (groupByOp && groupByOp->opcode == OP_SORT_GROUPBY)
    {
       /* no sorting required */
    }
    else
    {
      sortingOp = (ExecOperationNode *)malloc(sizeof(ExecOperationNode));
      sortingOp->opcode = OP_SORT_ORDERING;
      sortingOp->extraInfo = (void *)s->orderBy;

      sprintf(sortingOp->opcodeDescription,"sort order by");
    }
    if (sortingOp)
    {
      sortingOp->nextOp = plan;
      plan = sortingOp;
    }
  }

  printFinalPlan(plan);

}

#include "lex.yy.c"
int yyerror(char *s)
{
  printf("yyerror : %s\n", s);
}

int main()
{
  s_stmt = (SelectStmtNode *)malloc(sizeof(SelectStmtNode));
  memset(s_stmt, 0, sizeof(SelectStmtNode));

  if (yyparse()) return 1; /* parse error */

  printStmtParseTree(s_stmt);
  LoadMetadata();

  if (!semanticAnalysis(s_stmt)) return 1;

  initialQueryPlan(s_stmt);

  buildOptimalQueryPlan(s_stmt);

  return 0;
}
