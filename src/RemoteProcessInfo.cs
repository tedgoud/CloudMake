using System;
using System.Runtime.CompilerServices;

namespace CloudManager
{
    public enum Action
    {
        RUN,
        KILL}

    ;

    /**
	 * Class that keeps information about remote processes.
	 */
    public class RemoteProcessInfo
    {
        private Action _action;
        private string _nodename;
        private string _name;
        private string _path;
        private string _exec;
        private string _args;

        public RemoteProcessInfo (Action action, string nodename, string name)
            : this (action, nodename, name, null, null, null)
        {
        }

        public RemoteProcessInfo (Action action, string nodename, string name, string path, string exec, string args)
        {
            _action = action;
            _nodename = nodename;
            _name = name;
            _path = path;
            _exec = exec;
            _args = args;
        }

        public override string ToString ()
        {
            string res = "Proc: " + _name + "@" + _nodename + Environment.NewLine;

            if (_action == Action.RUN) {
                res = "RUN " + res;
                res += "Path: " + _path + Environment.NewLine;
                res += "Exec: " + _exec + Environment.NewLine;
                res += "Args: " + _args + Environment.NewLine;
            } else {
                res = "KILL " + res;
            }

            return res;
        }

        public Action GetAction ()
        {
            return _action;
        }

        public string GetNodename ()
        {
            return _nodename;
        }

        public string GetName ()
        {
            return _name;
        }

        public string GetPath ()
        {
            return _path;
        }

        public string GetExec ()
        {
            return _exec;
        }

        public string GetArgs ()
        {
            return _args;
        }
    }
}

