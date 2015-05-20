%{

//TODO Put your favorite license here
		
// yacc source generated by ebnf2y[1]
// at 2015-05-20 14:30:18.068079652 +0200 CEST
//
//  $ ebnf2y -o ql.y -oe ql.ebnf -start StatementList -pkg ql -p _
//
// CAUTION: If this file is a Go source file (*.go), it was generated
// automatically by '$ go tool yacc' from a *.y file - DO NOT EDIT in that case!
// 
//   [1]: http://github.com/cznic/ebnf2y

package ql //TODO real package name

//TODO required only be the demo _dump function
import (
	"bytes"
	"fmt"
	"strings"

	"github.com/cznic/strutil"
)

%}

%union {
	item interface{} //TODO insert real field(s)
}

%token	_ANDAND
%token	_ANDNOT
%token	_EQ
%token	_FLOAT_LIT
%token	_GE
%token	_IDENTIFIER
%token	_IMAGINARY_LIT
%token	_INT_LIT
%token	_LE
%token	_LSH
%token	_NEQ
%token	_OROR
%token	_QL_PARAMETER
%token	_RSH
%token	_RUNE_LIT
%token	_STRING_LIT

%type	<item> 	/*TODO real type(s), if/where applicable */
	_ANDAND
	_ANDNOT
	_EQ
	_FLOAT_LIT
	_GE
	_IDENTIFIER
	_IMAGINARY_LIT
	_INT_LIT
	_LE
	_LSH
	_NEQ
	_OROR
	_QL_PARAMETER
	_RSH
	_RUNE_LIT
	_STRING_LIT

%token _ADD
%token _ALTER
%token _AND
%token _AS
%token _ASC
%token _BEGIN
%token _BETWEEN
%token _BIGINT
%token _BIGRAT
%token _BLOB
%token _BOOL
%token _BY
%token _BYTE
%token _COLUMN
%token _COMMIT
%token _COMPLEX128
%token _COMPLEX64
%token _CREATE
%token _DEFAULT
%token _DELETE
%token _DESC
%token _DISTINCT
%token _DROP
%token _DURATION
%token _EXISTS
%token _FALSE
%token _FLOAT
%token _FLOAT32
%token _FLOAT64
%token _FROM
%token _FULL
%token _GROUPBY
%token _IF
%token _IN
%token _INDEX
%token _INSERT
%token _INT
%token _INT16
%token _INT32
%token _INT64
%token _INT8
%token _INTO
%token _IS
%token _JOIN
%token _LEFT
%token _LIKE
%token _LIMIT
%token _NOT
%token _NULL
%token _OFFSET
%token _ON
%token _OR
%token _ORDER
%token _OUTER
%token _RIGHT
%token _ROLLBACK
%token _RUNE
%token _SELECT
%token _SET
%token _STRING
%token _TABLE
%token _TIME
%token _TRANSACTION
%token _TRUE
%token _TRUNCATE
%token _UINT
%token _UINT16
%token _UINT32
%token _UINT64
%token _UINT8
%token _UNIQUE
%token _UPDATE
%token _VALUES
%token _WHERE

%type	<item> 	/*TODO real type(s), if/where applicable */
	AlterTableStmt
	AlterTableStmt1
	Assignment
	AssignmentList
	AssignmentList1
	AssignmentList2
	BeginTransactionStmt
	Call
	Call1
	ColumnDef
	ColumnDef1
	ColumnDef11
	ColumnDef2
	ColumnName
	ColumnNameList
	ColumnNameList1
	ColumnNameList2
	CommitStmt
	Conversion
	CreateIndexStmt
	CreateIndexStmt1
	CreateIndexStmt2
	CreateTableStmt
	CreateTableStmt1
	CreateTableStmt2
	CreateTableStmt3
	DeleteFromStmt
	DeleteFromStmt1
	DropIndexStmt
	DropIndexStmt1
	DropTableStmt
	DropTableStmt1
	EmptyStmt
	Expression
	Expression1
	Expression11
	ExpressionList
	ExpressionList1
	ExpressionList2
	Factor
	Factor1
	Factor11
	Factor2
	Field
	Field1
	FieldList
	FieldList1
	FieldList2
	GroupByClause
	Index
	IndexName
	InsertIntoStmt
	InsertIntoStmt1
	InsertIntoStmt2
	JoinClause
	JoinClause1
	JoinClause2
	Limit
	Literal
	Offset
	Operand
	OrderBy
	OrderBy1
	OrderBy11
	Predicate
	Predicate1
	Predicate11
	Predicate12
	Predicate121
	Predicate13
	PrimaryExpression
	PrimaryFactor
	PrimaryFactor1
	PrimaryFactor11
	PrimaryTerm
	PrimaryTerm1
	PrimaryTerm11
	QualifiedIdent
	QualifiedIdent1
	RecordSet
	RecordSet1
	RecordSet11
	RecordSet2
	RecordSetList
	RecordSetList1
	RecordSetList2
	RollbackStmt
	SelectStmt
	SelectStmt1
	SelectStmt2
	SelectStmt3
	SelectStmt4
	SelectStmt5
	SelectStmt6
	SelectStmt7
	SelectStmt8
	Slice
	Slice1
	Slice2
	Start
	Statement
	StatementList
	StatementList1
	TableName
	Term
	Term1
	Term11
	TruncateTableStmt
	Type
	UnaryExpr
	UnaryExpr1
	UnaryExpr11
	UpdateStmt
	UpdateStmt1
	UpdateStmt2
	Values
	Values1
	Values2
	WhereClause

/*TODO %left, %right, ... declarations */

%start Start

%%

AlterTableStmt:
	_ALTER _TABLE TableName AlterTableStmt1
	{
		$$ = []AlterTableStmt{"ALTER", "TABLE", $3, $4} //TODO 1
	}

AlterTableStmt1:
	_ADD ColumnDef
	{
		$$ = []AlterTableStmt1{"ADD", $2} //TODO 2
	}
|	_DROP _COLUMN ColumnName
	{
		$$ = []AlterTableStmt1{"DROP", "COLUMN", $3} //TODO 3
	}

Assignment:
	ColumnName '=' Expression
	{
		$$ = []Assignment{$1, "=", $3} //TODO 4
	}

AssignmentList:
	Assignment AssignmentList1 AssignmentList2
	{
		$$ = []AssignmentList{$1, $2, $3} //TODO 5
	}

AssignmentList1:
	/* EMPTY */
	{
		$$ = []AssignmentList1(nil) //TODO 6
	}
|	AssignmentList1 ',' Assignment
	{
		$$ = append($1.([]AssignmentList1), ",", $3) //TODO 7
	}

AssignmentList2:
	/* EMPTY */
	{
		$$ = nil //TODO 8
	}
|	','
	{
		$$ = "," //TODO 9
	}

BeginTransactionStmt:
	_BEGIN _TRANSACTION
	{
		$$ = []BeginTransactionStmt{"BEGIN", "TRANSACTION"} //TODO 10
	}

Call:
	'(' Call1 ')'
	{
		$$ = []Call{"(", $2, ")"} //TODO 11
	}

Call1:
	/* EMPTY */
	{
		$$ = nil //TODO 12
	}
|	ExpressionList
	{
		$$ = $1 //TODO 13
	}

ColumnDef:
	ColumnName Type ColumnDef1 ColumnDef2
	{
		$$ = []ColumnDef{$1, $2, $3, $4} //TODO 14
	}

ColumnDef1:
	/* EMPTY */
	{
		$$ = nil //TODO 15
	}
|	ColumnDef11
	{
		$$ = $1 //TODO 16
	}

ColumnDef11:
	_NOT _NULL
	{
		$$ = []ColumnDef11{"NOT", "NULL"} //TODO 17
	}
|	Expression
	{
		$$ = $1 //TODO 18
	}

ColumnDef2:
	/* EMPTY */
	{
		$$ = nil //TODO 19
	}
|	_DEFAULT Expression
	{
		$$ = []ColumnDef2{"DEFAULT", $2} //TODO 20
	}

ColumnName:
	_IDENTIFIER
	{
		$$ = $1 //TODO 21
	}

ColumnNameList:
	ColumnName ColumnNameList1 ColumnNameList2
	{
		$$ = []ColumnNameList{$1, $2, $3} //TODO 22
	}

ColumnNameList1:
	/* EMPTY */
	{
		$$ = []ColumnNameList1(nil) //TODO 23
	}
|	ColumnNameList1 ',' ColumnName
	{
		$$ = append($1.([]ColumnNameList1), ",", $3) //TODO 24
	}

ColumnNameList2:
	/* EMPTY */
	{
		$$ = nil //TODO 25
	}
|	','
	{
		$$ = "," //TODO 26
	}

CommitStmt:
	_COMMIT
	{
		$$ = "COMMIT" //TODO 27
	}

Conversion:
	Type '(' Expression ')'
	{
		$$ = []Conversion{$1, "(", $3, ")"} //TODO 28
	}

CreateIndexStmt:
	_CREATE CreateIndexStmt1 _INDEX CreateIndexStmt2 IndexName _ON TableName '(' ExpressionList ')'
	{
		$$ = []CreateIndexStmt{"CREATE", $2, "INDEX", $4, $5, "ON", $7, "(", $9, ")"} //TODO 29
	}

CreateIndexStmt1:
	/* EMPTY */
	{
		$$ = nil //TODO 30
	}
|	_UNIQUE
	{
		$$ = "UNIQUE" //TODO 31
	}

CreateIndexStmt2:
	/* EMPTY */
	{
		$$ = nil //TODO 32
	}
|	_IF _NOT _EXISTS
	{
		$$ = []CreateIndexStmt2{"IF", "NOT", "EXISTS"} //TODO 33
	}

CreateTableStmt:
	_CREATE _TABLE CreateTableStmt1 TableName '(' ColumnDef CreateTableStmt2 CreateTableStmt3 ')'
	{
		$$ = []CreateTableStmt{"CREATE", "TABLE", $3, $4, "(", $6, $7, $8, ")"} //TODO 34
	}

CreateTableStmt1:
	/* EMPTY */
	{
		$$ = nil //TODO 35
	}
|	_IF _NOT _EXISTS
	{
		$$ = []CreateTableStmt1{"IF", "NOT", "EXISTS"} //TODO 36
	}

CreateTableStmt2:
	/* EMPTY */
	{
		$$ = []CreateTableStmt2(nil) //TODO 37
	}
|	CreateTableStmt2 ',' ColumnDef
	{
		$$ = append($1.([]CreateTableStmt2), ",", $3) //TODO 38
	}

CreateTableStmt3:
	/* EMPTY */
	{
		$$ = nil //TODO 39
	}
|	','
	{
		$$ = "," //TODO 40
	}

DeleteFromStmt:
	_DELETE _FROM TableName DeleteFromStmt1
	{
		$$ = []DeleteFromStmt{"DELETE", "FROM", $3, $4} //TODO 41
	}

DeleteFromStmt1:
	/* EMPTY */
	{
		$$ = nil //TODO 42
	}
|	WhereClause
	{
		$$ = $1 //TODO 43
	}

DropIndexStmt:
	_DROP _INDEX DropIndexStmt1 IndexName
	{
		$$ = []DropIndexStmt{"DROP", "INDEX", $3, $4} //TODO 44
	}

DropIndexStmt1:
	/* EMPTY */
	{
		$$ = nil //TODO 45
	}
|	_IF _EXISTS
	{
		$$ = []DropIndexStmt1{"IF", "EXISTS"} //TODO 46
	}

DropTableStmt:
	_DROP _TABLE DropTableStmt1 TableName
	{
		$$ = []DropTableStmt{"DROP", "TABLE", $3, $4} //TODO 47
	}

DropTableStmt1:
	/* EMPTY */
	{
		$$ = nil //TODO 48
	}
|	_IF _EXISTS
	{
		$$ = []DropTableStmt1{"IF", "EXISTS"} //TODO 49
	}

EmptyStmt:
	/* EMPTY */
	{
		$$ = nil //TODO 50
	}

Expression:
	Term Expression1
	{
		$$ = []Expression{$1, $2} //TODO 51
	}

Expression1:
	/* EMPTY */
	{
		$$ = []Expression1(nil) //TODO 52
	}
|	Expression1 Expression11 Term
	{
		$$ = append($1.([]Expression1), $2, $3) //TODO 53
	}

Expression11:
	_OROR
	{
		$$ = $1 //TODO 54
	}
|	_OR
	{
		$$ = "OR" //TODO 55
	}

ExpressionList:
	Expression ExpressionList1 ExpressionList2
	{
		$$ = []ExpressionList{$1, $2, $3} //TODO 56
	}

ExpressionList1:
	/* EMPTY */
	{
		$$ = []ExpressionList1(nil) //TODO 57
	}
|	ExpressionList1 ',' Expression
	{
		$$ = append($1.([]ExpressionList1), ",", $3) //TODO 58
	}

ExpressionList2:
	/* EMPTY */
	{
		$$ = nil //TODO 59
	}
|	','
	{
		$$ = "," //TODO 60
	}

Factor:
	PrimaryFactor Factor1 Factor2
	{
		$$ = []Factor{$1, $2, $3} //TODO 61
	}

Factor1:
	/* EMPTY */
	{
		$$ = []Factor1(nil) //TODO 62
	}
|	Factor1 Factor11 PrimaryFactor
	{
		$$ = append($1.([]Factor1), $2, $3) //TODO 63
	}

Factor11:
	_GE
	{
		$$ = $1 //TODO 64
	}
|	'>'
	{
		$$ = ">" //TODO 65
	}
|	_LE
	{
		$$ = $1 //TODO 66
	}
|	'<'
	{
		$$ = "<" //TODO 67
	}
|	_NEQ
	{
		$$ = $1 //TODO 68
	}
|	_EQ
	{
		$$ = $1 //TODO 69
	}
|	_LIKE
	{
		$$ = "LIKE" //TODO 70
	}

Factor2:
	/* EMPTY */
	{
		$$ = nil //TODO 71
	}
|	Predicate
	{
		$$ = $1 //TODO 72
	}

Field:
	Expression Field1
	{
		$$ = []Field{$1, $2} //TODO 73
	}

Field1:
	/* EMPTY */
	{
		$$ = nil //TODO 74
	}
|	_AS _IDENTIFIER
	{
		$$ = []Field1{"AS", $2} //TODO 75
	}

FieldList:
	Field FieldList1 FieldList2
	{
		$$ = []FieldList{$1, $2, $3} //TODO 76
	}

FieldList1:
	/* EMPTY */
	{
		$$ = []FieldList1(nil) //TODO 77
	}
|	FieldList1 ',' Field
	{
		$$ = append($1.([]FieldList1), ",", $3) //TODO 78
	}

FieldList2:
	/* EMPTY */
	{
		$$ = nil //TODO 79
	}
|	','
	{
		$$ = "," //TODO 80
	}

GroupByClause:
	_GROUPBY ColumnNameList
	{
		$$ = []GroupByClause{"GROUP BY", $2} //TODO 81
	}

Index:
	'[' Expression ']'
	{
		$$ = []Index{"[", $2, "]"} //TODO 82
	}

IndexName:
	_IDENTIFIER
	{
		$$ = $1 //TODO 83
	}

InsertIntoStmt:
	_INSERT _INTO TableName InsertIntoStmt1 InsertIntoStmt2
	{
		$$ = []InsertIntoStmt{"INSERT", "INTO", $3, $4, $5} //TODO 84
	}

InsertIntoStmt1:
	/* EMPTY */
	{
		$$ = nil //TODO 85
	}
|	'(' ColumnNameList ')'
	{
		$$ = []InsertIntoStmt1{"(", $2, ")"} //TODO 86
	}

InsertIntoStmt2:
	Values
	{
		$$ = $1 //TODO 87
	}
|	SelectStmt
	{
		$$ = $1 //TODO 88
	}

JoinClause:
	JoinClause1 JoinClause2 _JOIN RecordSet _ON Expression
	{
		$$ = []JoinClause{$1, $2, "JOIN", $4, "ON", $6} //TODO 89
	}

JoinClause1:
	_LEFT
	{
		$$ = "LEFT" //TODO 90
	}
|	_RIGHT
	{
		$$ = "RIGHT" //TODO 91
	}
|	_FULL
	{
		$$ = "FULL" //TODO 92
	}

JoinClause2:
	/* EMPTY */
	{
		$$ = nil //TODO 93
	}
|	_OUTER
	{
		$$ = "OUTER" //TODO 94
	}

Limit:
	_LIMIT Expression
	{
		$$ = []Limit{"Limit", $2} //TODO 95
	}

Literal:
	_FALSE
	{
		$$ = "FALSE" //TODO 96
	}
|	_NULL
	{
		$$ = "NULL" //TODO 97
	}
|	_TRUE
	{
		$$ = "TRUE" //TODO 98
	}
|	_FLOAT_LIT
	{
		$$ = $1 //TODO 99
	}
|	_IMAGINARY_LIT
	{
		$$ = $1 //TODO 100
	}
|	_INT_LIT
	{
		$$ = $1 //TODO 101
	}
|	_RUNE_LIT
	{
		$$ = $1 //TODO 102
	}
|	_STRING_LIT
	{
		$$ = $1 //TODO 103
	}
|	_QL_PARAMETER
	{
		$$ = $1 //TODO 104
	}

Offset:
	_OFFSET Expression
	{
		$$ = []Offset{"OFFSET", $2} //TODO 105
	}

Operand:
	Literal
	{
		$$ = $1 //TODO 106
	}
|	QualifiedIdent
	{
		$$ = $1 //TODO 107
	}
|	'(' Expression ')'
	{
		$$ = []Operand{"(", $2, ")"} //TODO 108
	}

OrderBy:
	_ORDER _BY ExpressionList OrderBy1
	{
		$$ = []OrderBy{"ORDER", "BY", $3, $4} //TODO 109
	}

OrderBy1:
	/* EMPTY */
	{
		$$ = nil //TODO 110
	}
|	OrderBy11
	{
		$$ = $1 //TODO 111
	}

OrderBy11:
	_ASC
	{
		$$ = "ASC" //TODO 112
	}
|	_DESC
	{
		$$ = "DESC" //TODO 113
	}

Predicate:
	Predicate1
	{
		$$ = $1 //TODO 114
	}

Predicate1:
	Predicate11 Predicate12
	{
		$$ = []Predicate1{$1, $2} //TODO 115
	}
|	_IS Predicate13 _NULL
	{
		$$ = []Predicate1{"IS", $2, "NULL"} //TODO 116
	}

Predicate11:
	/* EMPTY */
	{
		$$ = nil //TODO 117
	}
|	_NOT
	{
		$$ = "NOT" //TODO 118
	}

Predicate12:
	_IN '(' ExpressionList ')'
	{
		$$ = []Predicate12{"IN", "(", $3, ")"} //TODO 119
	}
|	_IN '(' SelectStmt Predicate121 ')'
	{
		$$ = []Predicate12{"IN", "(", $3, $4, ")"} //TODO 120
	}
|	_BETWEEN PrimaryFactor _AND PrimaryFactor
	{
		$$ = []Predicate12{"BETWEEN", $2, "AND", $4} //TODO 121
	}

Predicate121:
	/* EMPTY */
	{
		$$ = nil //TODO 122
	}
|	';'
	{
		$$ = ";" //TODO 123
	}

Predicate13:
	/* EMPTY */
	{
		$$ = nil //TODO 124
	}
|	_NOT
	{
		$$ = "NOT" //TODO 125
	}

PrimaryExpression:
	Operand
	{
		$$ = $1 //TODO 126
	}
|	Conversion
	{
		$$ = $1 //TODO 127
	}
|	PrimaryExpression Index
	{
		$$ = []PrimaryExpression{$1, $2} //TODO 128
	}
|	PrimaryExpression Slice
	{
		$$ = []PrimaryExpression{$1, $2} //TODO 129
	}
|	PrimaryExpression Call
	{
		$$ = []PrimaryExpression{$1, $2} //TODO 130
	}

PrimaryFactor:
	PrimaryTerm PrimaryFactor1
	{
		$$ = []PrimaryFactor{$1, $2} //TODO 131
	}

PrimaryFactor1:
	/* EMPTY */
	{
		$$ = []PrimaryFactor1(nil) //TODO 132
	}
|	PrimaryFactor1 PrimaryFactor11 PrimaryTerm
	{
		$$ = append($1.([]PrimaryFactor1), $2, $3) //TODO 133
	}

PrimaryFactor11:
	'^'
	{
		$$ = "^" //TODO 134
	}
|	'|'
	{
		$$ = "|" //TODO 135
	}
|	'-'
	{
		$$ = "-" //TODO 136
	}
|	'+'
	{
		$$ = "+" //TODO 137
	}

PrimaryTerm:
	UnaryExpr PrimaryTerm1
	{
		$$ = []PrimaryTerm{$1, $2} //TODO 138
	}

PrimaryTerm1:
	/* EMPTY */
	{
		$$ = []PrimaryTerm1(nil) //TODO 139
	}
|	PrimaryTerm1 PrimaryTerm11 UnaryExpr
	{
		$$ = append($1.([]PrimaryTerm1), $2, $3) //TODO 140
	}

PrimaryTerm11:
	_ANDNOT
	{
		$$ = $1 //TODO 141
	}
|	'&'
	{
		$$ = "&" //TODO 142
	}
|	_LSH
	{
		$$ = $1 //TODO 143
	}
|	_RSH
	{
		$$ = $1 //TODO 144
	}
|	'%'
	{
		$$ = "%" //TODO 145
	}
|	'/'
	{
		$$ = "/" //TODO 146
	}
|	'*'
	{
		$$ = "*" //TODO 147
	}

QualifiedIdent:
	_IDENTIFIER QualifiedIdent1
	{
		$$ = []QualifiedIdent{$1, $2} //TODO 148
	}

QualifiedIdent1:
	/* EMPTY */
	{
		$$ = nil //TODO 149
	}
|	'.' _IDENTIFIER
	{
		$$ = []QualifiedIdent1{".", $2} //TODO 150
	}

RecordSet:
	RecordSet1 RecordSet2
	{
		$$ = []RecordSet{$1, $2} //TODO 151
	}

RecordSet1:
	TableName
	{
		$$ = $1 //TODO 152
	}
|	'(' SelectStmt RecordSet11 ')'
	{
		$$ = []RecordSet1{"(", $2, $3, ")"} //TODO 153
	}

RecordSet11:
	/* EMPTY */
	{
		$$ = nil //TODO 154
	}
|	';'
	{
		$$ = ";" //TODO 155
	}

RecordSet2:
	/* EMPTY */
	{
		$$ = nil //TODO 156
	}
|	_AS _IDENTIFIER
	{
		$$ = []RecordSet2{"AS", $2} //TODO 157
	}

RecordSetList:
	RecordSet RecordSetList1 RecordSetList2
	{
		$$ = []RecordSetList{$1, $2, $3} //TODO 158
	}

RecordSetList1:
	/* EMPTY */
	{
		$$ = []RecordSetList1(nil) //TODO 159
	}
|	RecordSetList1 ',' RecordSet
	{
		$$ = append($1.([]RecordSetList1), ",", $3) //TODO 160
	}

RecordSetList2:
	/* EMPTY */
	{
		$$ = nil //TODO 161
	}
|	','
	{
		$$ = "," //TODO 162
	}

RollbackStmt:
	_ROLLBACK
	{
		$$ = "ROLLBACK" //TODO 163
	}

SelectStmt:
	_SELECT SelectStmt1 SelectStmt2 _FROM RecordSetList SelectStmt3 SelectStmt4 SelectStmt5 SelectStmt6 SelectStmt7 SelectStmt8
	{
		$$ = []SelectStmt{"SELECT", $2, $3, "FROM", $5, $6, $7, $8, $9, $10, $11} //TODO 164
	}

SelectStmt1:
	/* EMPTY */
	{
		$$ = nil //TODO 165
	}
|	_DISTINCT
	{
		$$ = "DISTINCT" //TODO 166
	}

SelectStmt2:
	'*'
	{
		$$ = "*" //TODO 167
	}
|	FieldList
	{
		$$ = $1 //TODO 168
	}

SelectStmt3:
	/* EMPTY */
	{
		$$ = nil //TODO 169
	}
|	JoinClause
	{
		$$ = $1 //TODO 170
	}

SelectStmt4:
	/* EMPTY */
	{
		$$ = nil //TODO 171
	}
|	WhereClause
	{
		$$ = $1 //TODO 172
	}

SelectStmt5:
	/* EMPTY */
	{
		$$ = nil //TODO 173
	}
|	GroupByClause
	{
		$$ = $1 //TODO 174
	}

SelectStmt6:
	/* EMPTY */
	{
		$$ = nil //TODO 175
	}
|	OrderBy
	{
		$$ = $1 //TODO 176
	}

SelectStmt7:
	/* EMPTY */
	{
		$$ = nil //TODO 177
	}
|	Limit
	{
		$$ = $1 //TODO 178
	}

SelectStmt8:
	/* EMPTY */
	{
		$$ = nil //TODO 179
	}
|	Offset
	{
		$$ = $1 //TODO 180
	}

Slice:
	'[' Slice1 ':' Slice2 ']'
	{
		$$ = []Slice{"[", $2, ":", $4, "]"} //TODO 181
	}

Slice1:
	/* EMPTY */
	{
		$$ = nil //TODO 182
	}
|	Expression
	{
		$$ = $1 //TODO 183
	}

Slice2:
	/* EMPTY */
	{
		$$ = nil //TODO 184
	}
|	Expression
	{
		$$ = $1 //TODO 185
	}

Start:
	StatementList
	{
		_parserResult = $1 //TODO 186
	}

Statement:
	EmptyStmt
	{
		$$ = $1 //TODO 187
	}
|	AlterTableStmt
	{
		$$ = $1 //TODO 188
	}
|	BeginTransactionStmt
	{
		$$ = $1 //TODO 189
	}
|	CommitStmt
	{
		$$ = $1 //TODO 190
	}
|	CreateIndexStmt
	{
		$$ = $1 //TODO 191
	}
|	CreateTableStmt
	{
		$$ = $1 //TODO 192
	}
|	DeleteFromStmt
	{
		$$ = $1 //TODO 193
	}
|	DropIndexStmt
	{
		$$ = $1 //TODO 194
	}
|	DropTableStmt
	{
		$$ = $1 //TODO 195
	}
|	InsertIntoStmt
	{
		$$ = $1 //TODO 196
	}
|	RollbackStmt
	{
		$$ = $1 //TODO 197
	}
|	SelectStmt
	{
		$$ = $1 //TODO 198
	}
|	TruncateTableStmt
	{
		$$ = $1 //TODO 199
	}
|	UpdateStmt
	{
		$$ = $1 //TODO 200
	}

StatementList:
	Statement StatementList1
	{
		$$ = []StatementList{$1, $2} //TODO 201
	}

StatementList1:
	/* EMPTY */
	{
		$$ = []StatementList1(nil) //TODO 202
	}
|	StatementList1 ';' Statement
	{
		$$ = append($1.([]StatementList1), ";", $3) //TODO 203
	}

TableName:
	_IDENTIFIER
	{
		$$ = $1 //TODO 204
	}

Term:
	Factor Term1
	{
		$$ = []Term{$1, $2} //TODO 205
	}

Term1:
	/* EMPTY */
	{
		$$ = []Term1(nil) //TODO 206
	}
|	Term1 Term11 Factor
	{
		$$ = append($1.([]Term1), $2, $3) //TODO 207
	}

Term11:
	_ANDAND
	{
		$$ = $1 //TODO 208
	}
|	_AND
	{
		$$ = "AND" //TODO 209
	}

TruncateTableStmt:
	_TRUNCATE _TABLE TableName
	{
		$$ = []TruncateTableStmt{"TRUNCATE", "TABLE", $3} //TODO 210
	}

Type:
	_BIGINT
	{
		$$ = "bigint" //TODO 211
	}
|	_BIGRAT
	{
		$$ = "bigrat" //TODO 212
	}
|	_BLOB
	{
		$$ = "blob" //TODO 213
	}
|	_BOOL
	{
		$$ = "bool" //TODO 214
	}
|	_BYTE
	{
		$$ = "byte" //TODO 215
	}
|	_COMPLEX128
	{
		$$ = "complex128" //TODO 216
	}
|	_COMPLEX64
	{
		$$ = "complex64" //TODO 217
	}
|	_DURATION
	{
		$$ = "duration" //TODO 218
	}
|	_FLOAT
	{
		$$ = "float" //TODO 219
	}
|	_FLOAT32
	{
		$$ = "float32" //TODO 220
	}
|	_FLOAT64
	{
		$$ = "float64" //TODO 221
	}
|	_INT
	{
		$$ = "int" //TODO 222
	}
|	_INT16
	{
		$$ = "int16" //TODO 223
	}
|	_INT32
	{
		$$ = "int32" //TODO 224
	}
|	_INT64
	{
		$$ = "int64" //TODO 225
	}
|	_INT8
	{
		$$ = "int8" //TODO 226
	}
|	_RUNE
	{
		$$ = "rune" //TODO 227
	}
|	_STRING
	{
		$$ = "string" //TODO 228
	}
|	_TIME
	{
		$$ = "time" //TODO 229
	}
|	_UINT
	{
		$$ = "uint" //TODO 230
	}
|	_UINT16
	{
		$$ = "uint16" //TODO 231
	}
|	_UINT32
	{
		$$ = "uint32" //TODO 232
	}
|	_UINT64
	{
		$$ = "uint64" //TODO 233
	}
|	_UINT8
	{
		$$ = "uint8" //TODO 234
	}

UnaryExpr:
	UnaryExpr1 PrimaryExpression
	{
		$$ = []UnaryExpr{$1, $2} //TODO 235
	}

UnaryExpr1:
	/* EMPTY */
	{
		$$ = nil //TODO 236
	}
|	UnaryExpr11
	{
		$$ = $1 //TODO 237
	}

UnaryExpr11:
	'^'
	{
		$$ = "^" //TODO 238
	}
|	'!'
	{
		$$ = "!" //TODO 239
	}
|	'-'
	{
		$$ = "-" //TODO 240
	}
|	'+'
	{
		$$ = "+" //TODO 241
	}

UpdateStmt:
	_UPDATE TableName UpdateStmt1 AssignmentList UpdateStmt2
	{
		$$ = []UpdateStmt{"UPDATE", $2, $3, $4, $5} //TODO 242
	}

UpdateStmt1:
	/* EMPTY */
	{
		$$ = nil //TODO 243
	}
|	_SET
	{
		$$ = "SET" //TODO 244
	}

UpdateStmt2:
	/* EMPTY */
	{
		$$ = nil //TODO 245
	}
|	WhereClause
	{
		$$ = $1 //TODO 246
	}

Values:
	_VALUES '(' ExpressionList ')' Values1 Values2
	{
		$$ = []Values{"VALUES", "(", $3, ")", $5, $6} //TODO 247
	}

Values1:
	/* EMPTY */
	{
		$$ = []Values1(nil) //TODO 248
	}
|	Values1 ',' '(' ExpressionList ')'
	{
		$$ = append($1.([]Values1), ",", "(", $4, ")") //TODO 249
	}

Values2:
	/* EMPTY */
	{
		$$ = nil //TODO 250
	}
|	','
	{
		$$ = "," //TODO 251
	}

WhereClause:
	_WHERE Expression
	{
		$$ = []WhereClause{"WHERE", $2} //TODO 252
	}

%%

//TODO remove demo stuff below

var _parserResult interface{}

type (
	AlterTableStmt interface{}
	AlterTableStmt1 interface{}
	Assignment interface{}
	AssignmentList interface{}
	AssignmentList1 interface{}
	AssignmentList2 interface{}
	BeginTransactionStmt interface{}
	Call interface{}
	Call1 interface{}
	ColumnDef interface{}
	ColumnDef1 interface{}
	ColumnDef11 interface{}
	ColumnDef2 interface{}
	ColumnName interface{}
	ColumnNameList interface{}
	ColumnNameList1 interface{}
	ColumnNameList2 interface{}
	CommitStmt interface{}
	Conversion interface{}
	CreateIndexStmt interface{}
	CreateIndexStmt1 interface{}
	CreateIndexStmt2 interface{}
	CreateTableStmt interface{}
	CreateTableStmt1 interface{}
	CreateTableStmt2 interface{}
	CreateTableStmt3 interface{}
	DeleteFromStmt interface{}
	DeleteFromStmt1 interface{}
	DropIndexStmt interface{}
	DropIndexStmt1 interface{}
	DropTableStmt interface{}
	DropTableStmt1 interface{}
	EmptyStmt interface{}
	Expression interface{}
	Expression1 interface{}
	Expression11 interface{}
	ExpressionList interface{}
	ExpressionList1 interface{}
	ExpressionList2 interface{}
	Factor interface{}
	Factor1 interface{}
	Factor11 interface{}
	Factor2 interface{}
	Field interface{}
	Field1 interface{}
	FieldList interface{}
	FieldList1 interface{}
	FieldList2 interface{}
	GroupByClause interface{}
	Index interface{}
	IndexName interface{}
	InsertIntoStmt interface{}
	InsertIntoStmt1 interface{}
	InsertIntoStmt2 interface{}
	JoinClause interface{}
	JoinClause1 interface{}
	JoinClause2 interface{}
	Limit interface{}
	Literal interface{}
	Offset interface{}
	Operand interface{}
	OrderBy interface{}
	OrderBy1 interface{}
	OrderBy11 interface{}
	Predicate interface{}
	Predicate1 interface{}
	Predicate11 interface{}
	Predicate12 interface{}
	Predicate121 interface{}
	Predicate13 interface{}
	PrimaryExpression interface{}
	PrimaryFactor interface{}
	PrimaryFactor1 interface{}
	PrimaryFactor11 interface{}
	PrimaryTerm interface{}
	PrimaryTerm1 interface{}
	PrimaryTerm11 interface{}
	QualifiedIdent interface{}
	QualifiedIdent1 interface{}
	RecordSet interface{}
	RecordSet1 interface{}
	RecordSet11 interface{}
	RecordSet2 interface{}
	RecordSetList interface{}
	RecordSetList1 interface{}
	RecordSetList2 interface{}
	RollbackStmt interface{}
	SelectStmt interface{}
	SelectStmt1 interface{}
	SelectStmt2 interface{}
	SelectStmt3 interface{}
	SelectStmt4 interface{}
	SelectStmt5 interface{}
	SelectStmt6 interface{}
	SelectStmt7 interface{}
	SelectStmt8 interface{}
	Slice interface{}
	Slice1 interface{}
	Slice2 interface{}
	Start interface{}
	Statement interface{}
	StatementList interface{}
	StatementList1 interface{}
	TableName interface{}
	Term interface{}
	Term1 interface{}
	Term11 interface{}
	TruncateTableStmt interface{}
	Type interface{}
	UnaryExpr interface{}
	UnaryExpr1 interface{}
	UnaryExpr11 interface{}
	UpdateStmt interface{}
	UpdateStmt1 interface{}
	UpdateStmt2 interface{}
	Values interface{}
	Values1 interface{}
	Values2 interface{}
	WhereClause interface{}
)
	
func _dump() {
	s := fmt.Sprintf("%#v", _parserResult)
	s = strings.Replace(s, "%", "%%", -1)
	s = strings.Replace(s, "{", "{%i\n", -1)
	s = strings.Replace(s, "}", "%u\n}", -1)
	s = strings.Replace(s, ", ", ",\n", -1)
	var buf bytes.Buffer
	strutil.IndentFormatter(&buf, ". ").Format(s)
	buf.WriteString("\n")
	a := strings.Split(buf.String(), "\n")
	for _, v := range a {
		if strings.HasSuffix(v, "(nil)") || strings.HasSuffix(v, "(nil),") {
			continue
		}
	
		fmt.Println(v)
	}
}

// End of demo stuff
