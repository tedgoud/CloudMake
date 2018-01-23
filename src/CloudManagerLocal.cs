using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Policy;
using System.Threading;
using System.Xml;
using Vsync;
using System.Diagnostics;
using System.CodeDom.Compiler;

namespace CloudManager
{
    public class CloudManagerLocal
    {
        private string _vsyncAddress;
        private string _name;
        private string _curDirectory;
        private Dictionary<string, ProcessInfo> _procs;
        private object _procsLock;
        private List<List<string>> _leaders;
        private List<string> _reserveLeaders;
        private int _nreplicas;
        private List<CloudMakefile> _leaderCloudMakefiles;
        private CloudMakefile _localCloudMakefile;
        private DependencyStructure _dependencyStructure;
        private List<Tuple<MessageType,List<string>>> _queue;
        private object _queueLock;
        private string _debugFilename;
        private CloudMakeMonitor _cloudMakeMonitor;

        public enum MessageType
        {
            NEW_NODE = 0,
            FAIL_NODE = 1,
            MEMBER_JOIN = 2,
            LEADER_JOIN = 3,
            NEW_CONFIG = 4,
            RUN_PROCESS = 5,
            KILL_PROCESS = 6,
            UPDATE_CONFIG = 7,
            ASK_STATE = 8,
        }

        public CloudManagerLocal (List<CloudMakefile> leaderCloudMakefiles, CloudMakefile localCloudMakefile,
                                  string name, List<List<string>> leaders, List<string> reserveLeaders, int nreplicas,
                                  int timeout, Vsync.Group vsyncGroup, string debugFilename,
                                  string monitorDebugFilename)
        {
            _vsyncAddress = VsyncSystem.GetMyAddress ().ToStringVerboseFormat ();
            _name = name;
            _curDirectory = Directory.GetCurrentDirectory ();
            _procs = new Dictionary<string, ProcessInfo> ();
            _procsLock = new object ();
            _leaders = leaders;
            _reserveLeaders = reserveLeaders;
            _nreplicas = nreplicas;
            _leaderCloudMakefiles = leaderCloudMakefiles;
            _localCloudMakefile = localCloudMakefile;
            _queue = new List<Tuple<MessageType, List<string>>> ();
            _queueLock = new object ();
            _debugFilename = debugFilename;
            _cloudMakeMonitor = new CloudMakeMonitor (this, monitorDebugFilename, timeout);
            Thread cloudMakeMonitorThread = new Thread (delegate() {
                _cloudMakeMonitor.Run (vsyncGroup);
            });
            cloudMakeMonitorThread.Start ();
        }

        private void WriteMessage (MessageType type, List<string> parameters)
        {
            string str = type.ToString () + ":";

            foreach (string param in parameters)
                str += " " + param;
            CloudManager.WriteLine (_debugFilename, str);
        }

        private void ProcessMessages (Vsync.Group vsyncGroup, List<Tuple<MessageType, List<string>>> msgs)
        {
            foreach (Tuple<MessageType, List<string>> msg in msgs) {
                MessageType type = msg.Item1;
                List<string> parameters = msg.Item2;
                string vsyncAddress, procName, path, exec, args, filename, content, configFilename;
#if DEBUG
                WriteMessage (type, parameters);
#endif
                switch (type) {
                case MessageType.NEW_NODE:
                    if (parameters.Count != 1)
                        throw new Exception ("CloudMakeLocal: NEW_NODE: Does not accept " +
                        parameters.Count.ToString () + " parameters.");
                    vsyncAddress = parameters [0];
                    vsyncGroup.P2PSend (new Address (vsyncAddress), CloudManager.MEMBER_INFO, _vsyncAddress, _name);
                    break;
                case MessageType.FAIL_NODE:
                    break;
                case MessageType.MEMBER_JOIN:
                    MemberJoin (vsyncGroup);
                    break;
                case MessageType.LEADER_JOIN:
                    if (parameters.Count != 1)
                        throw new Exception ("CloudMakeLocal: LEADER_JOIN: Does not accept " +
                        parameters.Count.ToString () + " parameters.");
                    vsyncAddress = parameters [0];
                    CloudManager.AddLeader (_leaders, _reserveLeaders, _nreplicas, vsyncAddress);
                    break;
                case MessageType.NEW_CONFIG:
                    if (parameters.Count != 1)
                        throw new Exception ("CloudMakeLocal: NEW CONFIG: Should only have one configFilename.");
                    configFilename = parameters [0];
                    NewConfig (configFilename);
                    break;
                case MessageType.RUN_PROCESS:
                    procName = parameters [0];
                    path = parameters [1];
                    exec = parameters [2];
                    args = parameters [3];
                    RunProcess (procName, path, exec, args);
                    break;
                case MessageType.KILL_PROCESS:
                    procName = parameters [0];
                    KillProcess (procName);
                    break;
                case MessageType.UPDATE_CONFIG:
                    filename = parameters [0];
                    content = parameters [1];
                    UpdateConfig (filename, content);
                    break;
                case MessageType.ASK_STATE:
                    path = parameters [0];
                    filename = parameters [1];
                    AskState (vsyncGroup, path, filename);
                    break;
                default:
                    throw new Exception ("CloudMakeLocal: I should not have received " + msg.ToString () +
                    " message.");
                }
            }
        }

        private void MemberJoin (Vsync.Group vsyncGroup)
        {
            string memberDirectory = _name;
            string eventName = _name + "/new_node";
            string cloudMakeDirectory = memberDirectory + Path.DirectorySeparatorChar + "CloudMake";
            string cloudMakeConfigFilename = cloudMakeDirectory + Path.DirectorySeparatorChar.ToString () +
                                             "config.xml";
            List<Tuple<string, int>> newNodeEntries;
            HashSet<int> nameSet, cloudMakeConfigSet;

            if (_localCloudMakefile == null)
                throw new Exception ("CloudMakeLocal: There is no CloudMakefile.");
            nameSet = _localCloudMakefile.Compatible (memberDirectory); 
            if (nameSet.Count > 0) {
                Directory.SetCurrentDirectory (_name);
#if DEBUG
                CloudManager.WriteLine (_debugFilename, "Create Directory: " + memberDirectory);
#endif
                Directory.CreateDirectory (memberDirectory);
                cloudMakeConfigSet = _localCloudMakefile.Compatible (cloudMakeConfigFilename, nameSet);
                if (cloudMakeConfigSet.Count > 0) {
#if DEBUG
                    CloudManager.WriteLine (_debugFilename, "Create Directory: " + cloudMakeDirectory);
                    CloudManager.WriteLine (_debugFilename, "Added CloudMake Config: " + cloudMakeConfigFilename);
#endif
                    Directory.CreateDirectory (cloudMakeDirectory);
                    _dependencyStructure.AddCloudMakeConfigFile (cloudMakeConfigFilename);
                }
                Directory.SetCurrentDirectory (_curDirectory);
                newNodeEntries = _localCloudMakefile.FindMatchingEntries (eventName);
                if (newNodeEntries.Count > 1)
                    throw new Exception ("It is impossible for a NEW_NODE event to have more than one corresponding " +
                    "DFA.");
                if (newNodeEntries.Count == 1) {
                    _dependencyStructure.AddEntry (newNodeEntries [0]);
                    _dependencyStructure.AddTokenToEntry (newNodeEntries [0].Item1);
#if DEBUG
                    CloudManager.WriteLine (_debugFilename, "Added Token to New Node Event: " + eventName);
#endif
                    RunCloudMake (vsyncGroup);
                }
            } else {
                throw new Exception ("CloudMakeLocal: There is no matching entry for " + _name + "/new_node");
            }
        }

        private void NewConfig (string configFilename)
        {
            List<Tuple<string,int>> newNodeEntries = _localCloudMakefile.FindMatchingEntries (configFilename);

            _dependencyStructure.AddConfigFile (configFilename);
            foreach (Tuple<string,int> entry in newNodeEntries) {
                _dependencyStructure.AddEntry (entry);
                _dependencyStructure.AddTokenToEntry (entry.Item1);
            }
        }

        private void RunProcess (string procName, string path, string exec, string args)
        {
            ProcessInfo procInfo = new ProcessInfo (procName, path, exec, args, _debugFilename, _curDirectory);

            lock (_procsLock) {
                if (!_procs.ContainsKey (procName)) {
#if DEBUG
                    CloudManager.WriteLine (_debugFilename, Directory.GetCurrentDirectory ());
                    CloudManager.WriteLine (_debugFilename, "Run Process " + procName + ".");
                    CloudManager.WriteLine (_debugFilename, procInfo.ToString ());
#endif
                    procInfo.LaunchProcess ();
                    _procs.Add (procName, procInfo);
                } else {
#if DEBUG
                    CloudManager.WriteLine (_debugFilename, "Process " + procName + " already exists.");
#endif
                }
            }
        }

        private void KillProcess (string procName)
        {
            lock (_procsLock) {
                if (_procs.ContainsKey (procName)) {
#if DEBUG
                    CloudManager.WriteLine (_debugFilename, "Kill Process " + procName + ".");
#endif
                    _procs [procName].KillProcess ();
                    _procs.Remove (procName);
                } else {
#if DEBUG
                    CloudManager.WriteLine (_debugFilename, "Process " + procName + " does not exist.");
#endif
                }
            }
        }

        private void UpdateState (Vsync.Group vsyncGroup, string filename, string content)
        {
            XmlDocument xmlDoc = new XmlDocument ();
            List<string> filenameList = new List<string> (filename.Split ('/'));

            filename = String.Join (Path.DirectorySeparatorChar.ToString (), filenameList);
            CloudManager.WriteLine (_debugFilename, filename);
            CloudManager.WriteLine (_debugFilename, content);
            xmlDoc.LoadXml (content);
            xmlDoc.Save (filename);
            RunCloudMake (vsyncGroup);
        }

        private void UpdateConfig (string filename, string content)
        {
            XmlDocument xmlDoc = new XmlDocument ();
            List<string> filenameList = new List<string> (filename.Split ('/'));

            filename = String.Join (Path.DirectorySeparatorChar.ToString (), filenameList);
            CloudManager.WriteLine (_debugFilename, filename);
            CloudManager.WriteLine (_debugFilename, content);
            xmlDoc.LoadXml (content);
            xmlDoc.Save (filename);
        }

        private void ModifiedState (Vsync.Group vsyncGroup, string path, string name)
        {
            XmlDocument xmlDoc = new XmlDocument ();
            string filename = _name + "/" + name;
            string content;

            xmlDoc.Load (path);
            content = xmlDoc.OuterXml;
            if (_localCloudMakefile.Compatible (filename).Count > 0)
                UpdateState (vsyncGroup, filename, content);
            for (int i = 0; i < _leaderCloudMakefiles.Count; i++)
                if (_leaderCloudMakefiles [i].Compatible (filename).Count > 0)
                    vsyncGroup.P2PSend (new Address (_leaders [i] [0]), CloudManager.UPDATE_STATE, filename, content);
        }

        private void AskState (Vsync.Group vsyncGroup, string path, string filename)
        {
            FileSystemWatcher watcher = new FileSystemWatcher ();

            watcher.Path = path;
            watcher.NotifyFilter = NotifyFilters.LastAccess | NotifyFilters.LastWrite | NotifyFilters.FileName
            | NotifyFilters.DirectoryName;
            watcher.Filter = filename;
            watcher.Changed += (s, e) => ModifiedState (vsyncGroup, e.FullPath, e.Name);
            watcher.EnableRaisingEvents = true;
        }

        private Tuple<HashSet<string>, List<RemoteProcessInfo>> ProcessCloudMakeConfigFile (Vsync.Group vsyncGroup)
        {
            string cloudMakeConfigFilename = _name + "/CloudMake/config.xml";
            XmlDocument xmlDoc = new XmlDocument ();
            XmlNode xmlNode;
            List<RemoteProcessInfo> processes = new List<RemoteProcessInfo> ();
            List<string> statePathList = new List<string> ();
            List<string> stateFilenameList = new List<string> ();
            Dictionary<int,HashSet<string>> configFiles = new Dictionary<int, HashSet<string>> ();
            HashSet<string> localConfigFiles = new HashSet<string> ();

            xmlDoc.Load (cloudMakeConfigFilename);
            xmlNode = xmlDoc.DocumentElement;
            foreach (XmlNode childNode in xmlNode.ChildNodes) {
                switch (childNode.Name) {
                case ("process"):
                    if (childNode.Attributes ["action"].Value == "Run") {
                        string name = childNode ["name"].InnerText;
                        string path = childNode ["path"].InnerText;
                        string exec = childNode ["exec"].InnerText;
                        string args = childNode ["args"].InnerText;
#if DEBUG
                        CloudManager.WriteLine (_debugFilename, "Run process " + name + ".");
#endif
                        processes.Add (new RemoteProcessInfo (Action.RUN, _name, name, path, exec, args));
                    } else if (childNode.Attributes ["action"].Value == "Kill") {
                        string name = childNode ["name"].InnerText;
#if DEBUG
                        CloudManager.WriteLine (_debugFilename, "Kill process " + name + ".");
#endif
                        processes.Add (new RemoteProcessInfo (Action.KILL, _name, name));
                    }
                    break;
                case ("stateFile"):
                    string statePath = childNode ["path"].InnerText;
                    string stateName = childNode ["name"].InnerText;

                    if (statePath == "")
                        statePath = _curDirectory;
                    else
                        statePath = _curDirectory + Path.DirectorySeparatorChar.ToString () + statePath;
                    statePathList.Add (statePath);
                    stateFilenameList.Add (stateName);
                    break;
                case ("configFile"):
                    string configPath = childNode ["path"].InnerText;
                    string configName = childNode ["name"].InnerText;
                    string configFilename = (configPath == "") ? _name + "/" + configName : _name + "/" +
                                            configPath + "/" + configName;

                    for (int i = 0; i < _leaderCloudMakefiles.Count; i++) {
                        if (_leaderCloudMakefiles [i].Compatible (configFilename).Count > 0) {
                            if (!configFiles.ContainsKey (i))
                                configFiles [i] = new HashSet<string> ();
                            configFiles [i].Add (configFilename);
                        }
                    }

                    if (_localCloudMakefile.Compatible (configFilename).Count > 0) {
                        NewConfig (configFilename);
                        localConfigFiles.Add (configFilename);
                    }
                    break;
                default:
                    throw new Exception ("CloudMakeLocal Config: Operation " + childNode.Name + " is not supported " +
                    "from CloudMake.");
                }
            }

            foreach (int i in configFiles.Keys)
                vsyncGroup.P2PSend (new Address (_leaders [i] [0]), CloudManager.NEW_CONFIGS,
                    new List<string> (configFiles [i]));

            for (int i = 0; i < statePathList.Count; i++)
                AskState (vsyncGroup, statePathList [i], stateFilenameList [i]);

            return new Tuple<HashSet<string>, List<RemoteProcessInfo>> (localConfigFiles, processes);
        }

        private void RunCloudMake (Vsync.Group vsyncGroup)
        {
            Tuple<HashSet<string>, List<RemoteProcessInfo>> processedInfo;
            HashSet<string> configFiles;
            HashSet<string> cloudMakeConfigFiles;
            HashSet<string> activeConfigFiles;
            List<RemoteProcessInfo> processes = null;

            Directory.SetCurrentDirectory (_name);
            _dependencyStructure.Run ();
            configFiles = _dependencyStructure.GetModifiedConfigFiles ();
            cloudMakeConfigFiles = _dependencyStructure.GetModifiedCloudMakeConfigFiles ();
            CloudManager.WriteLine (_debugFilename, _dependencyStructure.ToString ());
            foreach (string cloudMakeConfigFilename in cloudMakeConfigFiles)
                CloudManager.WriteLine (_debugFilename, cloudMakeConfigFilename);
            if (cloudMakeConfigFiles.Count > 1)
                throw new Exception ("CloudMakeLocal: There are more than one CloudMake config files.");
            if (cloudMakeConfigFiles.Count == 1) {
                processedInfo = ProcessCloudMakeConfigFile (vsyncGroup);
                activeConfigFiles = processedInfo.Item1;
                processes = processedInfo.Item2;
                foreach (string activeConfigFile in activeConfigFiles)
                    if (File.Exists (activeConfigFile))
                        configFiles.Add (activeConfigFile);
            }
#if DEBUG
            CloudManager.WriteLine (_debugFilename, "Active config files:");
            foreach (string filename in configFiles)
                CloudManager.WriteLine (_debugFilename, filename);
            CloudManager.WriteLine (_debugFilename, "");
            CloudManager.WriteLine (_debugFilename, "Active CloudMake config files:");
            foreach (string filename in cloudMakeConfigFiles)
                CloudManager.WriteLine (_debugFilename, filename);
            CloudManager.WriteLine (_debugFilename, "");
#endif
            foreach (string activeConfigFile in configFiles) {
                List<string> fields = new List<string> (activeConfigFile.Split ('/'));
                string filename = String.Join (Path.DirectorySeparatorChar.ToString (), fields);
                XmlDocument xmlDoc = new XmlDocument ();

                xmlDoc.Load (filename);
                UpdateConfig (filename, xmlDoc.OuterXml);
            }
            if (cloudMakeConfigFiles.Count == 1) {
                foreach (RemoteProcessInfo proc in processes) {
                    if (proc.GetAction () == Action.RUN)
                        RunProcess (proc.GetName (), proc.GetPath (), proc.GetExec (), proc.GetArgs ());
                    else if (proc.GetAction () == Action.KILL)
                        KillProcess (proc.GetName ());
                }
            }
            _dependencyStructure.Reset ();
            Directory.SetCurrentDirectory (_curDirectory);
        }

        public void AddMessage (Tuple<MessageType, List<string>> msg)
        {
            lock (_queueLock) {
                _queue.Add (msg);
                Monitor.PulseAll (_queueLock);
            }
        }

        public void RemoveProcess (string procName)
        {
            lock (_procsLock) {
                if (_procs.ContainsKey (procName))
                    _procs.Remove (procName);
            }
        }

        public Dictionary<string, ProcessInfo> GetProcesses ()
        {
            Dictionary<string, ProcessInfo> res;

            lock (_procsLock) {
                res = new Dictionary<string, ProcessInfo> (_procs);
            }

            return res;
        }

        public void Run (Vsync.Group vsyncGroup)
        {
            List<Tuple<MessageType, List<string>>> msgs;

            _dependencyStructure = _localCloudMakefile == null ? null :
                new DependencyStructure (_localCloudMakefile, _debugFilename);
            while (true) {
                lock (_queueLock) {
                    while (_queue.Count == 0)
                        Monitor.Wait (_queueLock);
                    msgs = new List<Tuple<MessageType, List<string>>> (_queue);
                    _queue.Clear ();
                }
                ProcessMessages (vsyncGroup, msgs);
            }
        }
    }
}
