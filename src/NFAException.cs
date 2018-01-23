using System;

namespace CloudManager
{
    public class NFAException : Exception
    {
        public NFAException ()
        {
        }

        public NFAException (string message) : base (message)
        {
        }
    }
}

