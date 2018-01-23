using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using Vsync;

namespace CloudManager
{
    /**
 * Class responsible for monitoring the state of the system.
 */
    public class CloudMakeMonitor
    {
        private CloudManagerLocal _cloudManagerLocal;
        private string _debugFilename;
        private int _timeout;

        public CloudMakeMonitor (CloudManagerLocal cloudMakeLocal, string debugFilename, int timeout)
        {
            _cloudManagerLocal = cloudMakeLocal;
            _debugFilename = debugFilename;
            _timeout = timeout;
        }

        public void Run (Vsync.Group vsyncGroup)
        {
            Dictionary<string,ProcessInfo> procs;

            while (true) {
                procs = _cloudManagerLocal.GetProcesses ();
                foreach (string procName in procs.Keys) {
                    ProcessInfo procInfo = procs [procName];
                    int procId = procInfo.GetId ();

                    try {
                        using (Process proc = Process.GetProcessById (procId)) {
                            if ((proc == null) || proc.HasExited) {
#if DEBUG
                                CloudManager.WriteLine (_debugFilename, "Process " + procName + " stopped.");
#endif
                                _cloudManagerLocal.RemoveProcess (procName);
                                // TODO: Send STOPPED_PROC message to the leader.
                            }
                        }
                        ;
                    } catch (ArgumentException) {
#if DEBUG
                        CloudManager.WriteLine (_debugFilename, "Process " + procName + " stopped.");
#endif
                        _cloudManagerLocal.RemoveProcess (procName);
                        // TODO: Send STOPPED_PROC message to the leader.
                    }
                }
                System.Threading.Thread.Sleep (_timeout);
            }
        }
    }
}

