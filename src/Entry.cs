using System;

namespace CloudManager
{
    public class Entry
    {
        private bool _isVar;
        private string _varName;
        private NFA _nfa;

        public Entry (string variable)
        {
            _isVar = true;
            _varName = variable;
        }

        public Entry (NFA nfa)
        {
            _isVar = false;
            _nfa = nfa;
        }

        public bool IsVar ()
        {
            return _isVar;
        }

        public string GetVar ()
        {
            return _varName;
        }

        public NFA GetNFA ()
        {
            return _nfa;
        }

        public override string ToString ()
        {
            if (_isVar)
                return "VARIABLE " + _varName + Environment.NewLine;
            else
                return "NFA:" + Environment.NewLine + _nfa.ToString ();
        }
    }
}

