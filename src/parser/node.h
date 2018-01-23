#ifndef __NODE_H__
#define __NODE_H__

#include <cstring>

// Definition of types.
enum Type { TVAR, TCHAR, TRANGE, TBRSEQ, TCHOICE, TNCHOICE, TUNION, TATOM, TASTERISK, TPLUS, TQUESTIONMARK, TSEQUENCE,
TNAME, TDIRPATH, TXMLPATH, TEVENT, TENTRY, TINPUTS, TOUTPUTS, TRULE, TSRULE, TNRULE, TACTION };

typedef struct Node {
	Type type;
	char c;
	int length;
	Node **children;
} Node;

Node *makeCharNode(char c);
Node *makeRangeNode(char s, char e);
void printNode(Node *node);

#endif
