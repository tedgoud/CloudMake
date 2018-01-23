using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using State = System.UInt32;

namespace CloudManager
{
    public class NFA
    {
        private HashSet<char> _S;
        private HashSet<State> _Q;
        private Dictionary<State,Dictionary<char,HashSet<State>>> _d;
        private State _finalState;

        public NFA ()
        {
            _S = new HashSet<char> ();
            _Q = new HashSet<State> ();
            _Q.Add (0);
            _d = new Dictionary<uint, Dictionary<char, HashSet<uint>>> ();
            _finalState = 0;
        }

        public HashSet<char> GetAlphabet ()
        {
            return _S;
        }

        public HashSet<State> GetStates ()
        {
            return _Q;
        }

        public Dictionary<State,Dictionary<char,HashSet<State>>> GetTransitions ()
        {
            return _d;
        }

        public State GetFinalState ()
        {
            return _finalState;
        }

        public void SetFinalState (State finalState)
        {
            _finalState = finalState;
        }

        override public string ToString ()
        {
            string res = "";

            res += "Q:";
            foreach (State s in _Q)
                res += " " + s.ToString ();
            res += Environment.NewLine + "S:";
            foreach (char c in _S)
                res += " " + c.ToString ();
            res += Environment.NewLine + "d:";
            foreach (State startState in _d.Keys)
                foreach (char c in _d[startState].Keys)
                    foreach (State endState in _d[startState][c])
                        res += Environment.NewLine + "(" + startState + "," + c + ") -> " + endState;
            res += Environment.NewLine + "Final State: " + _finalState.ToString () + Environment.NewLine;
            return res;
        }

        public State AddState ()
        {
            State res = (State)_Q.Count;

            _Q.Add (res);
            return res;
        }

        public void AddTransition (State srcState, char c, State dstState)
        {
            if (!_Q.Contains (srcState))
                throw new NFAException ("Source State " + srcState.ToString () + " does not exist in the current NFA");
            if (!_S.Contains (c))
                _S.Add (c);
            if (!_Q.Contains (dstState))
                throw new NFAException ("Destination State " + dstState.ToString () + " does not exist in the " +
                "current NFA");
            if (!_d.ContainsKey (srcState))
                _d [srcState] = new Dictionary<char, HashSet<State>> ();
            if (!_d [srcState].ContainsKey (c))
                _d [srcState] [c] = new HashSet<State> ();
            _d [srcState] [c].Add (dstState);
        }

        static private NFA EnsureLastNFA (List<Entry> entryList)
        {
            int last = entryList.Count - 1;

            if (entryList [last].IsVar ()) {
                entryList.Add (new Entry (new NFA ()));
                last += 1;
            }

            return entryList [last].GetNFA ();
        }

        public void SequentialComposition (NFA nfa)
        {
            Dictionary<State,Dictionary<char,HashSet<State>>> transitions = nfa.GetTransitions ();
            Dictionary<State,State> transform = new Dictionary<State,State> ();

            foreach (State state in nfa.GetStates()) {
                if (state == 0)
                    transform [state] = _finalState;
                else
                    transform [state] = AddState ();
            }

            foreach (State state1 in transitions.Keys)
                foreach (char c in transitions[state1].Keys)
                    foreach (State state2 in transitions[state1][c])
                        AddTransition (transform [state1], c, transform [state2]);

            _finalState = transform [nfa.GetFinalState ()];
        }

        static public List<Entry> ParseTreeNode (Node node)
        {
            List<Entry> entryList = new List<Entry> ();

            entryList.Add (new Entry (new NFA ()));
            ParseTreeNode (node, entryList);

            return entryList;
        }

        static private void ParseTreeNode (Node node, List<Entry> entryList)
        {
            List<Node> children = null;
            HashSet<char> alphabet = null;
            NFA curNFA = null;
            State state1, state2, chkState1, chkState2;

            switch (node.GetNodeType ()) {
            case Node.Type.TENTRY:
                children = node.GetChildren ();
                if (children [0].GetNodeType () != Node.Type.TDIRPATH)
                    throw new CloudMakefile.ParseException ("A TDIRPATH node is expected as the first node of a " +
                    "TENTRY node.");
                if (children [1].GetNodeType () != Node.Type.TNAME)
                    throw new CloudMakefile.ParseException ("A TNAME node is expected as the second node of a TENTRY " +
                    "node.");
                if (children [2].GetNodeType () != Node.Type.TXMLPATH)
                    throw new CloudMakefile.ParseException ("A TXMLPATH node is expected as the third node of a " +
                    "TENTRY node.");
                ParseTreeNode (children [0], entryList);
                ParseTreeNode (children [1], entryList);
                curNFA = EnsureLastNFA (entryList);
                state1 = curNFA.GetFinalState ();
                state2 = curNFA.AddState ();
                curNFA.AddTransition (state1, '.', state2);
                state1 = state2;
                state2 = curNFA.AddState ();
                curNFA.AddTransition (state1, 'x', state2);
                state1 = state2;
                state2 = curNFA.AddState ();
                curNFA.AddTransition (state1, 'm', state2);
                state1 = state2;
                state2 = curNFA.AddState ();
                curNFA.AddTransition (state1, 'l', state2);
                curNFA.SetFinalState (state2);
                ParseTreeNode (children [2], entryList);
                break;
            case Node.Type.TEVENT:
                children = node.GetChildren ();
                if (children [0].GetNodeType () != Node.Type.TDIRPATH)
                    throw new CloudMakefile.ParseException ("A TDIRPATH node is expected as the first node of a " +
                    "TEVENT node.");
                if (children [1].GetNodeType () != Node.Type.TNAME)
                    throw new CloudMakefile.ParseException ("A TNAME node is expected as the second node of a TEVENT " +
                    "node.");
                ParseTreeNode (children [0], entryList);
                ParseTreeNode (children [1], entryList);
                break;
            case Node.Type.TDIRPATH:
                children = node.GetChildren ();
                foreach (Node child in children) {
                    if (child.GetNodeType () != Node.Type.TNAME)
                        throw new CloudMakefile.ParseException ("A TNAME node is expected as the child node of a " +
                        "TDIRPATH node.");
                    ParseTreeNode (child, entryList);
                    curNFA = EnsureLastNFA (entryList);
                    state1 = curNFA.GetFinalState ();
                    state2 = curNFA.AddState ();
                    curNFA.AddTransition (state1, Path.DirectorySeparatorChar, state2);
                    curNFA.SetFinalState (state2);
                }
                break;
            case Node.Type.TXMLPATH:
                Node lastNode = new Node ("ASTERISK(ATOM(SEQUENCE(CHOICE(RANGE(CHAR(A),CHAR(Z)),RANGE(CHAR(a)," +
                    "CHAR(z)),RANGE(CHAR(0),CHAR(9)),CHAR(@),CHAR(_)))))");

                children = node.GetChildren ();
                foreach (Node child in children) {
                    if (child.GetNodeType () != Node.Type.TNAME)
                        throw new CloudMakefile.ParseException ("A TNAME node is expected as the child node of a " +
                        "TXMLPATH node.");
                    curNFA = EnsureLastNFA (entryList);
                    state1 = curNFA.GetFinalState ();
                    state2 = curNFA.AddState ();
                    curNFA.AddTransition (state1, '{', state2);
                    curNFA.SetFinalState (state2);
                    ParseTreeNode (child, entryList);
                    curNFA = EnsureLastNFA (entryList);
                    state1 = curNFA.GetFinalState ();
                    state2 = curNFA.AddState ();
                    curNFA.AddTransition (state1, '}', state2);
                    curNFA.SetFinalState (state2);
                }
                curNFA = EnsureLastNFA (entryList);
                chkState1 = curNFA.GetFinalState ();
                state1 = chkState1;
                state2 = curNFA.AddState ();
                curNFA.AddTransition (state1, '{', state2);
                curNFA.SetFinalState (state2);
                ParseTreeNode (lastNode, entryList);
                state1 = curNFA.GetFinalState ();
                state2 = curNFA.AddState ();
                chkState2 = state2;
                curNFA.AddTransition (state1, '}', state2);
                curNFA.SetFinalState (state2);
                curNFA.AddTransition (chkState1, Char.MinValue, chkState2);
                curNFA.AddTransition (chkState2, Char.MinValue, chkState1);
                break;
            case Node.Type.TNAME:
                children = node.GetChildren ();
                foreach (Node child in children) {
                    if (child.GetNodeType () == Node.Type.TSEQUENCE)
                        ParseTreeNode (child, entryList);
                    else if (child.GetNodeType () == Node.Type.TVAR)
                        ParseTreeNode (child, entryList);
                    else
                        throw new CloudMakefile.ParseException ("A TSEQUENCE or TVAR node is expected as the child " +
                        "node of a TNAME node.");
                }
                break;
            case Node.Type.TSEQUENCE:
                children = node.GetChildren ();
                foreach (Node child in children) {
                    if ((child.GetNodeType () != Node.Type.TASTERISK) && (child.GetNodeType () != Node.Type.TPLUS) &&
                    (child.GetNodeType () != Node.Type.TQUESTIONMARK) && (child.GetNodeType () != Node.Type.TCHAR) &&
                    (child.GetNodeType () != Node.Type.TCHOICE) && (child.GetNodeType () != Node.Type.TNO_CHOICE) &&
                    (child.GetNodeType () != Node.Type.TUNION) && (child.GetNodeType () != Node.Type.TATOM))
                        throw new CloudMakefile.ParseException ("An appropriate node is expected as the child node " +
                        "of a TSEQUENCE node.");
                    ParseTreeNode (child, entryList);
                }
                break;
            case Node.Type.TATOM:
                children = node.GetChildren ();
                if (children [0].GetNodeType () != Node.Type.TSEQUENCE)
                    throw new CloudMakefile.ParseException ("A TSEQUENCE node is expected as the first node of a " +
                    "TATOM node.");
                ParseTreeNode (children [0], entryList);
                break;
            case Node.Type.TASTERISK:
                children = node.GetChildren ();
                if ((children [0].GetNodeType () != Node.Type.TCHAR) &&
                (children [0].GetNodeType () != Node.Type.TCHOICE) &&
                (children [0].GetNodeType () != Node.Type.TNO_CHOICE) &&
                (children [0].GetNodeType () != Node.Type.TUNION) &&
                (children [0].GetNodeType () != Node.Type.TATOM))
                    throw new CloudMakefile.ParseException ("An appropriate node is expected as the child node of a " +
                    "TASTERISK node.");
                curNFA = EnsureLastNFA (entryList);
                chkState1 = curNFA.GetFinalState ();
                ParseTreeNode (children [0], entryList);
                chkState2 = curNFA.GetFinalState ();
                curNFA.AddTransition (chkState1, Char.MinValue, chkState2);
                curNFA.AddTransition (chkState2, Char.MinValue, chkState1);
                break;
            case Node.Type.TPLUS:
                children = node.GetChildren ();
                if ((children [0].GetNodeType () != Node.Type.TCHAR) &&
                (children [0].GetNodeType () != Node.Type.TCHOICE) &&
                (children [0].GetNodeType () != Node.Type.TNO_CHOICE) &&
                (children [0].GetNodeType () != Node.Type.TUNION) &&
                (children [0].GetNodeType () != Node.Type.TATOM))
                    throw new CloudMakefile.ParseException ("An appropriate node is expected as the child node of a " +
                    "TPLUS node.");
                curNFA = EnsureLastNFA (entryList);
                chkState1 = curNFA.GetFinalState ();
                ParseTreeNode (children [0], entryList);
                chkState2 = curNFA.GetFinalState ();
                curNFA.AddTransition (chkState2, Char.MinValue, chkState1);
                break;
            case Node.Type.TQUESTIONMARK:
                children = node.GetChildren ();
                if ((children [0].GetNodeType () != Node.Type.TCHAR) &&
                (children [0].GetNodeType () != Node.Type.TCHOICE) &&
                (children [0].GetNodeType () != Node.Type.TNO_CHOICE) &&
                (children [0].GetNodeType () != Node.Type.TUNION) &&
                (children [0].GetNodeType () != Node.Type.TATOM))
                    throw new CloudMakefile.ParseException ("An appropriate node is expected as the child node of a " +
                    "TQUESTIONMARK node.");
                curNFA = EnsureLastNFA (entryList);
                chkState1 = curNFA.GetFinalState ();
                ParseTreeNode (children [0], entryList);
                chkState2 = curNFA.GetFinalState ();
                curNFA.AddTransition (chkState1, Char.MinValue, chkState2);
                break;
            case Node.Type.TCHOICE:
                alphabet = new HashSet<char> ();
                children = node.GetChildren ();
                foreach (Node child in children) {
                    if ((child.GetNodeType () != Node.Type.TCHAR) && (child.GetNodeType () != Node.Type.TRANGE))
                        throw new CloudMakefile.ParseException ("A TCHAR and a TRANGE node is expected as the child " +
                        "node of a TCHOICE node.");
                    if (child.GetNodeType () == Node.Type.TRANGE) {
                        Tuple<char,char> range = child.GetRange ();

                        for (char c = range.Item1; c <= range.Item2; c++)
                            alphabet.Add (c);
                    } else {
                        alphabet.Add (child.GetChar ());
                    }
                }
                curNFA = EnsureLastNFA (entryList);
                state1 = curNFA.GetFinalState ();
                state2 = curNFA.AddState ();
                foreach (char c in alphabet)
                    curNFA.AddTransition (state1, c, state2);
                curNFA.SetFinalState (state2);
                break;
            case Node.Type.TNO_CHOICE:
                alphabet = new HashSet<char> ();
                for (char c = 'A'; c <= 'Z'; c++)
                    alphabet.Add (c);
                for (char c = 'a'; c <= 'z'; c++)
                    alphabet.Add (c);
                for (char c = '0'; c <= '9'; c++)
                    alphabet.Add (c);
                alphabet.Add ('_');
                alphabet.Add ('@');
                children = node.GetChildren ();
                foreach (Node child in children) {
                    if (child.GetNodeType () == Node.Type.TRANGE) {
                        Tuple<char,char> range = child.GetRange ();

                        for (char c = range.Item1; c <= range.Item2; c++)
                            alphabet.Remove (c);
                    } else {
                        alphabet.Remove (child.GetChar ());
                    }
                }
                state1 = curNFA.GetFinalState ();
                state2 = curNFA.AddState ();
                curNFA = EnsureLastNFA (entryList);
                foreach (char c in alphabet)
                    curNFA.AddTransition (state1, c, state2);
                curNFA.SetFinalState (state2);
                break;
            case Node.Type.TUNION:
                children = node.GetChildren ();
                if (children [0].GetNodeType () != Node.Type.TSEQUENCE)
                    throw new CloudMakefile.ParseException ("A TSEQUENCE node is expected as the first node of a " +
                    "TUNION node.");
                if (children [1].GetNodeType () != Node.Type.TSEQUENCE)
                    throw new CloudMakefile.ParseException ("A TSEQUENCE node is expected as the second node of a " +
                    "TUNION node.");
                curNFA = EnsureLastNFA (entryList);
                state1 = curNFA.GetFinalState ();
                ParseTreeNode (children [0], entryList);
                chkState1 = curNFA.GetFinalState ();
                curNFA.SetFinalState (state1);
                ParseTreeNode (children [1], entryList);
                chkState2 = curNFA.GetFinalState ();
                state2 = curNFA.AddState ();
                curNFA.AddTransition (chkState1, Char.MinValue, state2);
                curNFA.AddTransition (chkState2, Char.MinValue, state2);
                curNFA.SetFinalState (state2);
                break;
            case Node.Type.TVAR:
                string varName = CloudMakefile.ParseString (node);

                entryList.Add (new Entry (varName));
                break;
            case Node.Type.TCHAR:
                char character = node.GetChar ();

                curNFA = EnsureLastNFA (entryList);
                state1 = curNFA.GetFinalState ();
                state2 = curNFA.AddState ();
                curNFA.AddTransition (state1, character, state2);
                curNFA.SetFinalState (state2);
                break;
            default:
                throw new CloudMakefile.ParseException ("Unhandled node type: " + node.GetNodeType ().ToString ());
            }
        }

        private HashSet<State> EClosure (State state)
        {
            HashSet<State> states = new HashSet<State> (new State[] { state });

            return EClosure (states);
        }

        private HashSet<State> EClosure (HashSet<State> states)
        {
            HashSet<State> res = new HashSet<State> ();
            List<State> unsearched = new List<State> ();
            State curState;

            foreach (State state in states) {
                unsearched.Add (state);
                res.Add (state);
            }

            while (unsearched.Count > 0) {
                curState = unsearched [0];
                if ((_d.ContainsKey (curState)) && (_d [curState].ContainsKey (Char.MinValue))) {
                    foreach (State state in _d[curState][Char.MinValue]) {
                        if (!res.Contains (state)) {
                            unsearched.Add (state);
                            res.Add (state);
                        }
                    }
                }
                unsearched.Remove (curState);
            }
            return res;
        }

        bool ContainsStates (List<HashSet<State>> Q, HashSet<State> states)
        {
            foreach (HashSet<State> tempStates in Q)
                if (states.SetEquals (tempStates))
                    return true;
            return false;
        }

        public DFA ConvertToDFA ()
        {
            HashSet<char> S = _S;
            HashSet<State> tempStartState = EClosure (0);
            Dictionary<HashSet<State>, Dictionary<char,HashSet<State>>> tempD = 
                new Dictionary<HashSet<State>, Dictionary<char, HashSet<State>>> ();
            List<HashSet<State>> Q = new List<HashSet<State>> ();
            List<HashSet<State>> QUnmarked = new List<HashSet<State>> ();
            List<HashSet<State>> tempFinalStates = new List<HashSet<State>> ();
            DFA res = new DFA ();
            Dictionary<HashSet<State>,State> transform = new Dictionary<HashSet<State>, State> ();
            State state1;

            if (S.Contains (Char.MinValue))
                S.Remove (char.MinValue);

            Q.Add (tempStartState);
            QUnmarked.Add (tempStartState);
            while (QUnmarked.Count > 0) {
                HashSet<State> curState = QUnmarked [0];
                Dictionary<char,HashSet<State>> tempNestedD = new Dictionary<char, HashSet<State>> ();

                foreach (char c in GetAlphabet()) {
                    HashSet<State> tempNestedSet = new HashSet<State> ();

                    foreach (State startState in curState)
                        if ((_d.ContainsKey (startState)) && (_d [startState].ContainsKey (c)))
                            foreach (State endState in _d[startState][c])
                                tempNestedSet.Add (endState);
                    tempNestedSet = EClosure (tempNestedSet);
                    if (tempNestedSet.Count == 0)
                        continue;
                    if (!ContainsStates (Q, tempNestedSet)) {
                        QUnmarked.Add (tempNestedSet);
                        Q.Add (tempNestedSet);
                    }
                    tempNestedD.Add (c, tempNestedSet);
                }
                tempD.Add (curState, tempNestedD);
                QUnmarked.RemoveAt (0);
            }

            foreach (HashSet<State> states in Q)
                if (states.Contains (GetFinalState ()))
                    tempFinalStates.Add (states);

            foreach (HashSet<State> oldState in Q)
                transform.Add (oldState, res.AddState ());

            foreach (HashSet<State> startState in tempD.Keys) {
                foreach (HashSet<State> stateSet1 in transform.Keys) {
                    if (stateSet1.SetEquals (startState)) {
                        state1 = transform [stateSet1];
                        foreach (char c in tempD[startState].Keys)
                            foreach (HashSet<State> stateSet2 in transform.Keys)
                                if (stateSet2.SetEquals (tempD [stateSet1] [c]))
                                    res.AddTransition (state1, c, transform [stateSet2]);
                        break;
                    }
                }
            }

            foreach (HashSet<State> states in tempFinalStates)
                res.AddFinalState (transform [states]);

            return res;
        }
    }
}