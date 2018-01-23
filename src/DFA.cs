using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using State = System.UInt32;
using System.Runtime.InteropServices;

namespace CloudManager
{
    public class DFA
    {
        private HashSet<char> _S;
        private HashSet<State> _Q;
        private Dictionary <State, Dictionary<Char, State>> _d;
        private HashSet<State> _finalStates;

        public DFA ()
        {
            _S = new HashSet<char> ();
            _Q = new HashSet<State> ();
            _d = new Dictionary<State, Dictionary<char, State>> ();
            _finalStates = new HashSet<State> ();
        }

        public HashSet<char> GetAlphabet ()
        {
            return _S;
        }

        public HashSet<State> GetStates ()
        {
            return _Q;
        }

        public Dictionary<State,Dictionary<char,State>> GetTransitions ()
        {
            return _d;
        }

        public HashSet<State> GetFinalStates ()
        {
            return _finalStates;
        }

        override public string ToString ()
        {
            string res = "";

            res += "S:";
            foreach (char c in _S)
                res += " " + c.ToString ();
            res += Environment.NewLine + "Q:";
            foreach (State state in _Q)
                res += " " + state.ToString ();
            res += Environment.NewLine + "d:";
            foreach (State startState in _d.Keys)
                foreach (char c in _d[startState].Keys)
                    res += Environment.NewLine + "(" + startState.ToString () + "," + c.ToString () + ") -> " +
                    _d [startState] [c].ToString ();
            res += Environment.NewLine + "Final States :";
            foreach (State finalState in _finalStates)
                res += Environment.NewLine + finalState.ToString ();
            res += Environment.NewLine;
            return res;
        }

        public State AddState ()
        {
            State res = (State)_Q.Count;

            _Q.Add (res);
            return res;
        }

        public void AddFinalState (State finalState)
        {
            if (!_Q.Contains (finalState))
                throw new DFAException ("Final State " + finalState.ToString () + " does not exist in the current DFA");
            _finalStates.Add (finalState);
        }

        public void AddTransition (State srcState, char c, State dstState)
        {
            if (!_Q.Contains (srcState))
                throw new DFAException ("Source State " + srcState.ToString () + " does not exist in the current DFA");
            if (!_S.Contains (c))
                _S.Add (c);
            if (!_Q.Contains (dstState))
                throw new DFAException ("Destination State " + dstState.ToString () + " does not exist in the " +
                "current DFA");

            if (!_d.ContainsKey (srcState))
                _d [srcState] = new Dictionary<char, State> ();
            if (_d [srcState].ContainsKey (c)) {
                if (_d [srcState] [c] != dstState)
                    throw new DFAException ("Transition from " + srcState.ToString () + " with " + c.ToString () +
                    " already exists in the current DFA.");
            } else {
                _d [srcState] [c] = dstState;
            }
        }

        static public DFA operator * (DFA dfa1, DFA dfa2)
        {
            Dictionary <State, Dictionary<Char, State>> d1 = dfa1.GetTransitions ();
            Dictionary <State, Dictionary<Char, State>> d2 = dfa2.GetTransitions ();
            HashSet<State> finalStates1 = dfa1.GetFinalStates ();
            HashSet<State> finalStates2 = dfa2.GetFinalStates ();
            List<Tuple<State,State>> unexploredStates = new List<Tuple<State,State>> ();
            Dictionary<Tuple<State,State>,State> transform = new Dictionary<Tuple<State,State>, State> ();
            Tuple<State,State> tempInitialState = new Tuple<State,State> (0, 0);
            DFA res = new DFA ();
            State state1;

            unexploredStates.Add (tempInitialState);
            transform.Add (tempInitialState, res.AddState ());
            while (unexploredStates.Count != 0) {
                Tuple<State,State> tempStartState = unexploredStates [0];
                State startState1 = tempStartState.Item1;
                State startState2 = tempStartState.Item2;

                state1 = transform [tempStartState];
                if ((finalStates1.Contains (tempStartState.Item1)) && (finalStates2.Contains (tempStartState.Item2)))
                    res.AddFinalState (state1);
                if (d1.ContainsKey (startState1) && d2.ContainsKey (startState2)) {
                    Dictionary<char, State> nestedD1 = d1 [startState1];
                    Dictionary<char, State> nestedD2 = d2 [startState2];

                    foreach (char c in nestedD1.Keys) {
                        if (nestedD2.ContainsKey (c)) {
                            Tuple<State,State> tempEndState = new Tuple<State,State> (nestedD1 [c], nestedD2 [c]);

                            if (!transform.ContainsKey (tempEndState)) {
                                unexploredStates.Add (tempEndState);
                                transform.Add (tempEndState, res.AddState ());
                            }
                            res.AddTransition (state1, c, transform [tempEndState]);
                        }
                    }
                }
                unexploredStates.RemoveAt (0);
            }
            return Reduce (res);
        }

        static public DFA operator - (DFA dfa1, DFA dfa2)
        {
            Dictionary <State, Dictionary<Char, State>> d1 = dfa1.GetTransitions ();
            Dictionary <State, Dictionary<Char, State>> d2 = dfa2.GetTransitions ();
            HashSet<State> finalStates1 = dfa1.GetFinalStates ();
            HashSet<State> finalStates2 = dfa2.GetFinalStates ();
            List<Tuple<State,State>> unexploredStates = new List<Tuple<State,State>> ();
            Dictionary<Tuple<State,State>,State> transform = new Dictionary<Tuple<State,State>, State> ();
            Tuple<State,State> tempInitialState = new Tuple<State,State> (0, 0);
            DFA res = new DFA ();
            State state1;

            unexploredStates.Add (tempInitialState);
            transform.Add (tempInitialState, res.AddState ());
            while (unexploredStates.Count != 0) {
                Tuple<State,State> tempStartState = unexploredStates [0];
                State startState1 = tempStartState.Item1;
                State startState2 = tempStartState.Item2;

                state1 = transform [tempStartState];
                if ((finalStates1.Contains (tempStartState.Item1)) && (!finalStates2.Contains (tempStartState.Item2)))
                    res.AddFinalState (state1);
                if (d1.ContainsKey (startState1)) {
                    Dictionary<char, State> nestedD1 = d1 [startState1];
                    Dictionary<char, State> nestedD2 = (d2.ContainsKey (startState2)) ? d2 [startState2] :
						new Dictionary<char, State> ();

                    foreach (char c in nestedD1.Keys) {
                        if (nestedD2.ContainsKey (c)) {
                            Tuple<State,State> tempEndState = new Tuple<State,State> (nestedD1 [c], nestedD2 [c]);

                            if (!transform.ContainsKey (tempEndState)) {
                                unexploredStates.Add (tempEndState);
                                transform.Add (tempEndState, res.AddState ());
                            }
                            res.AddTransition (state1, c, transform [tempEndState]);
                        } else {
                            Tuple<State,State> tempEndState = new Tuple<State,State> (nestedD1 [c], UInt32.MaxValue);

                            if (!transform.ContainsKey (tempEndState)) {
                                unexploredStates.Add (tempEndState);
                                transform.Add (tempEndState, res.AddState ());
                            }
                            res.AddTransition (state1, c, transform [tempEndState]);
                        }
                    }
                }
                unexploredStates.RemoveAt (0);
            }
            return Reduce (res);
        }

        private static DFA Reduce (DFA dfa)
        {
            DFA res = dfa;

            if (res != null)
                res = res.RemoveRedudantStates ();
            if (res != null)
                res = res.Minimize ();

            return res;
        }

        public DFA Minimize ()
        {
            List<HashSet<State>> P = new List<HashSet<State>> ();
            List<HashSet<State>> W = new List<HashSet<State>> ();
            Dictionary<State, HashSet<State>> transformA = new Dictionary<State, HashSet<State>> ();
            Dictionary<HashSet<State>, State> transformB = new Dictionary<HashSet<State>, State> ();
            DFA res = new DFA ();
            State state1;

            P.Add (_finalStates);
            P.Add (new HashSet<State> (_Q));
            P [1].ExceptWith (_finalStates);
            W.Add (_finalStates);

            while (W.Count > 0) {
                foreach (char c in _S) {
                    HashSet<State> X = new HashSet<State> ();

                    foreach (State state in _d.Keys)
                        if ((_d [state].ContainsKey (c)) && (W [0].Contains (_d [state] [c])))
                            X.Add (state);

                    if (X.Count == 0)
                        continue;

                    List<HashSet<State>> tempP = new List<HashSet<State>> (P);

                    foreach (HashSet<State> Y in P) {
                        HashSet<State> Y1 = new HashSet<State> (Y);
                        HashSet<State> Y2 = new HashSet<State> (Y);

                        Y1.IntersectWith (X);
                        Y2.ExceptWith (X);
                        if ((Y1.Count > 0) && (Y2.Count > 0)) {
                            tempP.Remove (Y);
                            tempP.Add (Y1);
                            tempP.Add (Y2);
                            if (W.Contains (Y)) {
                                W.Remove (Y);
                                W.Add (Y1);
                                W.Add (Y2);
                            } else {
                                if (Y1.Count <= Y2.Count)
                                    W.Add (Y1);
                                else
                                    W.Add (Y2);
                            }
                        }
                    }
                    P = tempP;
                }
                W.RemoveAt (0);
            }

            foreach (State state in _Q) {
                foreach (HashSet<State> tempSet in P) {
                    if (tempSet.Contains (state))
                        transformA.Add (state, tempSet);
                }
                if (!transformA.ContainsKey (state))
                    throw new Exception ("Something is wrong with Hopcroft algorithm.");
            }

            state1 = res.AddState ();
            foreach (HashSet<State> tempSet in P) {
                if (tempSet.Contains (0))
                    transformB.Add (tempSet, state1);
                else
                    transformB.Add (tempSet, res.AddState ());
            }

            foreach (State startState in _d.Keys) {
                State tempStartState = transformB [transformA [startState]];

                foreach (char c in _d[startState].Keys) {
                    State tempEndState = transformB [transformA [_d [startState] [c]]];

                    res.AddTransition (tempStartState, c, tempEndState);
                }
            }

            foreach (State finalState in _finalStates)
                res.AddFinalState (transformB [transformA [finalState]]);

            return res;
        }

        public DFA RemoveRedudantStates ()
        {
            HashSet<State> redudantStates = new HashSet<State> ();
            Dictionary<State,State> transform = new Dictionary<State, State> ();
            DFA res = new DFA ();
            State state1;

            state1 = res.AddState ();
            foreach (State state in _Q) {
                if (!ReachesFinalStates (state)) {
                    redudantStates.Add (state);
                } else {
                    if (state == 0)
                        transform.Add (0, state1);
                    else
                        transform.Add (state, res.AddState ());
                }
            }

            if (redudantStates.Contains (0))
                return null;

            foreach (State startState in _d.Keys) {
                if (redudantStates.Contains (startState))
                    continue;

                state1 = transform [startState];
                foreach (char c in _d[startState].Keys) {
                    if (redudantStates.Contains (_d [startState] [c]))
                        continue;
                    res.AddTransition (state1, c, transform [_d [startState] [c]]);
                }
            }

            foreach (State finalState in _finalStates)
                res.AddFinalState (transform [finalState]);

            return res;
        }

        private bool ReachesFinalStates (State state)
        {
            if (!_Q.Contains (state))
                throw new Exception ("State does not exist in the DFA.");

            if (_finalStates.Contains (state))
                return true;

            List<State> reachableStates = new List<State> ();
            List<State> unexploredStates = new List<State> ();
            State curState, tempState;

            reachableStates.Add (state);
            unexploredStates.Add (state);
            while (unexploredStates.Count > 0) {
                curState = unexploredStates [0];
                if (_d.ContainsKey (curState)) {
                    foreach (char c in _d[curState].Keys) {
                        tempState = _d [curState] [c];
                        if (reachableStates.Contains (tempState))
                            continue;
                        if (_finalStates.Contains (tempState))
                            return true;
                        reachableStates.Add (tempState);
                        unexploredStates.Add (tempState);
                    }
                }
                unexploredStates.RemoveAt (0);
            }

            return false;
        }

        public bool EqualState (List<State> states1, List<State> states2)
        {
            if (states1.Count != states2.Count)
                return false;

            states1.Sort ();
            states2.Sort ();
            for (int i = 0; i < states1.Count; i++)
                if (states1 [i] != states2 [i])
                    return false;
            return true;
        }

        public FAResponse Parse (string filename)
        {
            char[] filenameChars = filename.ToCharArray ();
            State curState = 0;

            foreach (char c in filenameChars) {
                if ((_d.ContainsKey (curState)) && (_d [curState].ContainsKey (c)))
                    curState = _d [curState] [c];
                else
                    return FAResponse.REJECT;
            }

            if (_finalStates.Contains (curState))
                return FAResponse.ACCEPT;
            else
                return FAResponse.NOT_REJECT;
        }

        public bool ParseLocal (string nodename)
        {
            char[] nodenameChars = nodename.ToCharArray ();
            State curState = 0;

            foreach (char c in nodenameChars) {
                if (!((_d.ContainsKey (curState)) && (_d [curState].ContainsKey (c)) && (_d [curState].Count == 1)))
                    return false;
                curState = _d [curState] [c];
            }
            return true;
        }

        public string GetNode ()
        {
            string res = "";
            State curState = 0;
            char c = '0';
            IEnumerator enumerator;

            if (!((_d.ContainsKey (curState)) && (_d [curState].Count == 1)))
                return null;
            enumerator = _d [curState].Keys.GetEnumerator ();
            enumerator.MoveNext ();
            c = Convert.ToChar (enumerator.Current);
            curState = _d [curState] [c];

            while (c != '/') {
                res += c.ToString ();
                if (!((_d.ContainsKey (curState)) && (_d [curState].Count == 1)))
                    return null;
                enumerator = _d [curState].Keys.GetEnumerator ();
                enumerator.MoveNext ();
                c = Convert.ToChar (enumerator.Current);
                curState = _d [curState] [c];
            }
            return res;
        }
    }
}

