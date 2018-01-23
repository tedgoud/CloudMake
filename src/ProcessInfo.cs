using System;
using System.Diagnostics;
using System.IO;

namespace CloudManager
{
    /**
	 * Process Information
	 */
    public class ProcessInfo
    {
        private int _id;
        private string _name;
        private string _path;
        private string _exec;
        private string _args;
        private string _debugFilename;

        public ProcessInfo (string name, string path, string exec, string args, string debugFilename,
                            string curDirectory)
        {
            _name = name;
            if ((!Path.IsPathRooted (path)) && (path != ""))
                _path = curDirectory + Path.DirectorySeparatorChar.ToString () + path;
            else if (Path.IsPathRooted (path))
                _path = path;
            else
                _path = curDirectory;
            _exec = exec;
            _args = args;
            _debugFilename = debugFilename;
        }

        public override string ToString ()
        {
            string res = "Proc: " + _name + Environment.NewLine;

            res += "Path: " + _path + Environment.NewLine;
            res += "Exec: " + _exec + Environment.NewLine;
            res += "Args: " + _args + Environment.NewLine;

            return res;
        }

        public int GetId ()
        {
            return _id;
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

        public void LaunchProcess ()
        {
            ProcessStartInfo startInfo = new ProcessStartInfo ();

            startInfo.WorkingDirectory = _path;
            startInfo.FileName = _exec;
            startInfo.Arguments = _args;

#if DEBUG
            CloudManager.WriteLine (_debugFilename, "Directory: " + startInfo.WorkingDirectory);
            CloudManager.WriteLine (_debugFilename, "Call " + startInfo.FileName + " " + startInfo.Arguments);
#endif

            using (Process proc = Process.Start (startInfo)) {
                _id = proc.Id;
            }
            ;
        }

        public void KillProcess ()
        {
            using (Process proc = Process.GetProcessById (_id)) {
                proc.Kill ();
            }
        }
    }
}

