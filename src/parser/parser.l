%{
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include "node.h"
#include "parser.tab.h"
using namespace std;
%}

%%

"/"     {return SLASH;}
"("     {return LEFT_PARENTHESIS;}
")"     {return RIGHT_PARENTHESIS;}
"["     {return LEFT_BRACKET_1;}
"]"     {return RIGHT_BRACKET_1;}
"{"     {return LEFT_BRACKET_2;}
"}"     {return RIGHT_BRACKET_2;}
"+"     {return PLUS;}
"-"     {return MINUS;}
"*"     {return ASTERISK;}
"?"     {return QUESTIONMARK;}
"|"     {return BAR;}
"^"     {return CIRCUMFLEX_ACCENT;}
":"     {return COLON;}
"$"		{return DOLLAR;}
","     {return COMMA;}
"\t"	{return TAB;}
" "		{return SPACE;}
[\r\n]+	{return EOL;}
FOREACH {return FOREACH;}
IN		{return IN;}
DO		{return DO;}
.xml    {return XML_EXTENSION;}
[A-Z]   {yylval.val = strdup(yytext)[0]; return CLETTER;}
[a-z]   {yylval.val = strdup(yytext)[0]; return SLETTER;}
[0-9]   {yylval.val = strdup(yytext)[0]; return DIGIT;}
[_@:]   {yylval.val = strdup(yytext)[0]; return OTHER_CHAR;}
.		{yylval.val = strdup(yytext)[0]; return ANY;}

%%
