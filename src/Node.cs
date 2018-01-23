using System;
using System.Collections;
using System.Collections.Generic;

namespace CloudManager
{
    public class Node
    {
        private Type _type;
        private char _c;
        private List<Node> _children;

        public enum Type
        {
            TVAR,
            TCHAR,
            TRANGE,
            TCHOICE,
            TNO_CHOICE,
            TUNION,
            TATOM,
            TASTERISK,
            TPLUS,
            TQUESTIONMARK,
            TSEQUENCE,
            TNAME,
            TDIRPATH,
            TXMLPATH,
            TEVENT,
            TENTRY,
            TINPUTS,
            TOUTPUTS,
            TRULE,
            TSRULE,
            TNRULE,
            TACTION,
        };

        public Node (string nodeString)
        {
            int start = nodeString.IndexOf ('(');
            int end = nodeString.LastIndexOf (')');
            string typeString = nodeString.Substring (0, start);
            string contentString = nodeString.Substring (start + 1, end - start - 1);
            List<string> childrenStrings;

            if (typeString == "CHAR") {
                _type = Type.TCHAR;
                if (contentString.Length != 1)
                    throw new CloudMakefile.ParseException ("Wrong Input to CHAR Node.");
                _c = contentString [0];
                _children = null;
            } else {
                childrenStrings = SplitContent (contentString);
                switch (typeString) {
                case("VAR"):
                    _type = Type.TVAR;
                    break;
                case("RANGE"):
                    _type = Type.TRANGE;
                    if (childrenStrings.Count != 2)
                        throw new CloudMakefile.ParseException ("Wrong Number of Inputs to RANGE Node.");
                    break;
                case("CHOICE"):
                    _type = Type.TCHOICE;
                    break;
                case("NO_CHOICE"):
                    _type = Type.TNO_CHOICE;
                    break;
                case("UNION"):
                    _type = Type.TUNION;
                    if (childrenStrings.Count != 2)
                        throw new CloudMakefile.ParseException ("Wrong Number of Inputs to UNION Node.");
                    break;
                case("ATOM"):
                    _type = Type.TATOM;
                    if (childrenStrings.Count != 1)
                        throw new CloudMakefile.ParseException ("Wrong Number of Inputs to TATOM Node.");
                    break;
                case("ASTERISK"):
                    _type = Type.TASTERISK;
                    if (childrenStrings.Count != 1)
                        throw new CloudMakefile.ParseException ("Wrong Number of Inputs to ASTERISK Node.");
                    break;
                case("PLUS"):
                    _type = Type.TPLUS;
                    if (childrenStrings.Count != 1)
                        throw new CloudMakefile.ParseException ("Wrong Number of Inputs to PLUS Node.");
                    break;
                case("QUESTIONMARK"):
                    _type = Type.TQUESTIONMARK;
                    if (childrenStrings.Count != 1)
                        throw new CloudMakefile.ParseException ("Wrong Number of Inputs to QUESTIONMARK Node.");
                    break;
                case("SEQUENCE"):
                    _type = Type.TSEQUENCE;
                    break;
                case("NAME"):
                    _type = Type.TNAME;
                    break;
                case("DIRPATH"):
                    _type = Type.TDIRPATH;
                    break;
                case("XMLPATH"):
                    _type = Type.TXMLPATH;
                    break;
                case("EVENT"):
                    _type = Type.TEVENT;
                    if (childrenStrings.Count != 2)
                        throw new CloudMakefile.ParseException ("Wrong Number of Inputs to EVENT Node.");
                    break;
                case("ENTRY"):
                    _type = Type.TENTRY;
                    if (childrenStrings.Count != 3)
                        throw new CloudMakefile.ParseException ("Wrong Number of Inputs to ENTRY Node.");
                    break;
                case("INPUTS"):
                    _type = Type.TINPUTS;
                    break;
                case("OUTPUTS"):
                    _type = Type.TOUTPUTS;
                    break;
                case ("RULE"):
                    _type = Type.TRULE;
                    if (childrenStrings.Count != 3)
                        throw new CloudMakefile.ParseException ("Wrong Number of Inputs to RULE Node.");
                    break;
                case("SRULE"):
                    _type = Type.TSRULE;
                    if (childrenStrings.Count != 3)
                        throw new CloudMakefile.ParseException ("Wrong Number of Inputs to SRULE Node.");
                    break;
                case("NRULE"):
                    _type = Type.TNRULE;
                    if (childrenStrings.Count != 3)
                        throw new CloudMakefile.ParseException ("Wrong Number of Inputs to NRULE Node.");
                    break;
                case("ACTION"):
                    _type = Type.TACTION;
                    break;
                default:
                    throw new CloudMakefile.ParseException (typeString + " is a Wrong Type of Node");
                }
                _children = new List<Node> ();
                for (int i = 0; i < childrenStrings.Count; i++)
                    _children.Add (new Node (childrenStrings [i]));
            }
        }

        private List<string> SplitContent (string contentString)
        {
            List<string> res = new List<string> ();
            int countLeftPar = 0;
            int countRightPar = 0;
            int start = 0;
            int i = 0;
            string temp;

            while (i != contentString.Length) {
                if ((i >= 5) && (contentString.Substring (i - 5, 5) == "CHAR(")) {
                    i++;
                    continue;
                }
                if (contentString [i] == '(')
                    countLeftPar += 1;
                else if (contentString [i] == ')')
                    countRightPar += 1;
                if ((countLeftPar != 0) && (countLeftPar == countRightPar)) {
                    temp = contentString.Substring (start, i - start + 1);
                    res.Add (temp);
                    countLeftPar = 0;
                    countRightPar = 0;
                    start = i + 2;
                }
                i++;
            }
            return res;
        }

        public Type GetNodeType ()
        {
            return _type;
        }

        public char GetChar ()
        {
            if (!(_type == Type.TCHAR))
                throw new CloudMakefile.ParseException ("You asked a char from type " + _type.ToString ());
            return _c;
        }

        public Tuple<char,char> GetRange ()
        {
            if (!(_type == Type.TRANGE))
                return null;
            return new Tuple<char, char> (_children [0].GetChar (), _children [1].GetChar ());
        }

        public List<Node> GetChildren ()
        {
            return _children;
        }

        public override string ToString ()
        {
            string res;

            res = _type.ToString () + "(";
            if (_type == Type.TCHAR)
                res += _c.ToString ();
            else {
                if (_children.Count != 0) {
                    for (int i = 0; i < _children.Count - 1; i++)
                        res += _children [i].ToString () + ",";
                    res += _children [_children.Count - 1].ToString ();
                }
            }
            res += ")";

            return res;
        }
    }
}

