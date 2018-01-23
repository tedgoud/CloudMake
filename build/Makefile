CLOUD_MAKE_HOME=$(realpath ../)
SRC_DIR=$(CLOUD_MAKE_HOME)/src
PARSER_DIR=$(SRC_DIR)/parser
EXT_DIR=$(CLOUD_MAKE_HOME)/ext
VSYNC_DIR=$(EXT_DIR)/vsync
BIN_DIR=$(CLOUD_MAKE_HOME)/bin

all: $(BIN_DIR)/CloudMake

$(BIN_DIR)/CloudMake: $(SRC_DIR)/*.cs $(EXT_DIR)/Vsync.dll $(BIN_DIR)/parser
	mcs -t:exe -r:$(EXT_DIR)/CommandLine.dll,$(EXT_DIR)/mono.posix.dll,$(EXT_DIR)/Vsync.dll -out:$(BIN_DIR)/CloudMake $(SRC_DIR)/*.cs

$(EXT_DIR)/Vsync.dll: $(VSYNC_DIR)/Vsync.cs
	mcs -t:library -out:$(EXT_DIR)/Vsync.dll $(VSYNC_DIR)/Vsync.cs

$(PARSER_DIR)/parser.tab.c $(PARSER_DIR)/parser.tab.h : $(PARSER_DIR)/parser.y $(PARSER_DIR)/node.h
	bison -o $(PARSER_DIR)/parser.tab.c -vd $(PARSER_DIR)/parser.y

$(PARSER_DIR)/parser.lex.c : $(PARSER_DIR)/parser.l $(PARSER_DIR)/parser.tab.h
	flex -o $(PARSER_DIR)/parser.lex.c -l $(PARSER_DIR)/parser.l 

$(BIN_DIR)/parser: $(PARSER_DIR)/parser.lex.c $(PARSER_DIR)/parser.tab.c $(PARSER_DIR)/parser.tab.h
	g++ -o $(BIN_DIR)/parser $(PARSER_DIR)/parser.lex.c $(PARSER_DIR)/parser.tab.c -lfl

clean:
	rm -f $(BIN_DIR)/* $(EXT_DIR)/Vsync.dll $(PARSER_DIR)/parser.tab.* $(PARSER_DIR)/parser.lex $(PARSER_DIR)/parser.output
