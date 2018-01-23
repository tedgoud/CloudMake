using System;

namespace CloudManager
{
    public class DFAException : Exception
    {
        public DFAException ()
        {
        }

        public DFAException (string message) : base (message)
        {
        }
    }
}

