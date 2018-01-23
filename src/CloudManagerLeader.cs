using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Dynamic;
using System.IO;
using System.Net.Mime;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Xml;
using Vsync;
using System.Configuration;
using System.Media;
using System.Net;
using System.Collections.Concurrent;
using System.Resources;

namespace CloudManager
{
    public class CloudManagerLeader
    {
        // Vsync Address
        private string _vsyncAddress;
        // Leader Information
        private List<List<string>> _leaders;
        private List<string> _reserveLeaders;
        private int _nreplicas;
        private State _state;
        private string _workingDir;
        // Corresponding CloudMakefiles
        private List<CloudMakefile> _leaderCloudMakefiles;
        private Dictionary<string,CloudMakefile> _localCloudMakefiles;
        private int _partition;
        private int _rank;
        private DependencyStructure _dependencyStructure;
        // Members
        private Dictionary<string, string> _nameToAddress;
        private Dictionary<string, string> _addressToName;
        // Message Queue
        private List<Tuple<MessageType,List<string>>> _queue;
        private List<Tuple<MessageType,List<string>>> _pending;
        private int _updatesDelivered;
        // Locks
        private object _queueLock;
        // Debug Parameters
        private string _debugFilename;

        public enum State
        {
            INITIAL = 0,
            WAITFORSTATE = 1,
            ACTIVE = 2,
        }

        public enum MessageType
        {
            NEW_NODE = 0,
            FAIL_NODE = 1,
            MEMBER_JOIN = 2,
            MEMBER_LEAVE = 3,
            LEADER_JOIN = 4,
            NEW_MEMBER = 5,
            NEW_CONFIG = 6,
            STOPPED_PROC = 7,
            UPDATE_STATE = 8,
            ACK = 9,
            RUN_CLOUD_MAKE = 10,
            STATE_TRANSFER = 11,
            SEND_PENDING = 12,
        }

        public CloudManagerLeader (List<CloudMakefile> leaderCloudMakefiles,
                                   Dictionary<string,CloudMakefile> localCloudMakefiles,
                                   Dictionary<string, string> nameToAddress, Dictionary<string,string> addressToName,
                                   List<List<string>> leaders, List<string> reserveLeaders, int nreplicas,
                                   string workingDir, string debugFilename)
        {
            _vsyncAddress = VsyncSystem.GetMyAddress ().ToStringVerboseFormat ();
            _leaders = leaders;
            _reserveLeaders = reserveLeaders;
            _nreplicas = nreplicas;
            _leaderCloudMakefiles = leaderCloudMakefiles;
            _localCloudMakefiles = localCloudMakefiles;
            _partition = CloudManager.FindPartition (_leaders, _reserveLeaders, _vsyncAddress);
            if (_partition >= 0) {
                _rank = _leaders [_partition].IndexOf (_vsyncAddress);
                _state = _rank == 0 ? State.ACTIVE : State.WAITFORSTATE;
            } else if (_partition == -1) {
                _rank = _reserveLeaders.IndexOf (_vsyncAddress);
                _state = State.INITIAL;
            } else {
                throw new Exception ("CloudMakeLeader: I cannot find myself.\n");
            }
            _nameToAddress = nameToAddress;
            _addressToName = addressToName;
            _queue = new List<Tuple<MessageType, List<string>>> ();
            _queueLock = new object ();
            _pending = new List<Tuple<MessageType,List<string>>> ();
            _updatesDelivered = 0;
            _debugFilename = debugFilename;
            _workingDir = workingDir;
            if (!Directory.Exists (_workingDir))
                throw new Exception ("CloudMakeLeader: CloudMakeLeader directory " + _workingDir + " does not exist.");
            Directory.SetCurrentDirectory (_workingDir);
        }

        private void FindInitialEntries ()
        {
            List<Tuple<string,int>> entries = _leaderCloudMakefiles [_partition].FindMatchingEntries ("");

            foreach (Tuple<string, int> entry in entries)
                _dependencyStructure.AddEntry (entry);
        }

        private void WriteMessage (MessageType type, List<string> parameters)
        {
            string str = "Leader " + type.ToString () + ":";

            foreach (string param in parameters)
                str += " " + param;
            CloudManager.WriteLine (_debugFilename, str);
        }

        public void AddMessage (Tuple<MessageType, List<string>> msg)
        {
            lock (_queueLock) {
                _queue.Add (msg);
                Monitor.PulseAll (_queueLock);
            }
        }

        private static void EnsureDir (List<string> path)
        {
            if (path.Count == 0)
                return;

            string dirname = path [0];
            int cur = 1;

            while ((cur < path.Count) && (Directory.Exists (dirname))) {
                dirname += Path.DirectorySeparatorChar.ToString () + path [cur];
                cur++;
            }

            while (cur < path.Count) {
                Directory.CreateDirectory (dirname);
                dirname += Path.DirectorySeparatorChar.ToString () + path [cur];
                cur++;
            }
        }

        private void NewMember (string memberName)
        {
            string memberDirectory = memberName;
            string eventName = memberName + "/new_node";
            string cloudMakeDirectory = memberDirectory + Path.DirectorySeparatorChar + "CloudMake";
            string cloudMakeConfigFilename = cloudMakeDirectory + Path.DirectorySeparatorChar.ToString () +
                                             "config.xml";
            List<Tuple<string,int>> newNodeEntries;
            HashSet<int> nodenameEntries, cloudMakefileEntries;

            nodenameEntries = _leaderCloudMakefiles [_partition].Compatible (memberName);
            foreach (int entry in nodenameEntries)
                CloudManager.WriteLine (_debugFilename, "Entry: " + entry.ToString ());
            if (nodenameEntries.Count > 0) {
                Directory.CreateDirectory (memberDirectory);
#if DEBUG
                CloudManager.WriteLine (_debugFilename, "Created Directory: " + memberDirectory);
#endif
                cloudMakefileEntries = _leaderCloudMakefiles [_partition].Compatible (cloudMakeConfigFilename,
                    nodenameEntries);
                if (cloudMakefileEntries.Count > 0) {
                    Directory.CreateDirectory (cloudMakeDirectory);
                    _dependencyStructure.AddCloudMakeConfigFile (cloudMakeConfigFilename);
#if DEBUG
                    CloudManager.WriteLine (_debugFilename, "Created Directory: " + cloudMakeDirectory);
                    CloudManager.WriteLine (_debugFilename, "Added CloudMake Config: " + cloudMakeConfigFilename);
#endif
                }
                newNodeEntries = _leaderCloudMakefiles [_partition].FindMatchingEntries (eventName);
                if (newNodeEntries.Count > 1)
                    throw new Exception ("It is impossible for a NEW_NODE event to have more than one corresponding " +
                    "DFA.");
                if (newNodeEntries.Count == 1) {
                    _dependencyStructure.AddEntry (newNodeEntries [0]);
                    _dependencyStructure.AddTokenToEntry (newNodeEntries [0].Item1);
#if DEBUG
                    CloudManager.WriteLine (_debugFilename, "Added Token to New Node Event: " + eventName);
#endif
                }
            }
        }

        private void NewConfig (string configFilename)
        {
            _dependencyStructure.AddConfigFile (configFilename);
        }

        private bool IsEventInPartition (string eventName)
        {
            return _leaderCloudMakefiles [_partition].Compatible (eventName).Count >= 1;
        }

        private void MemberLeave (string memberName)
        {
            string eventName = memberName + "/fail_node";
            List<Tuple<string,int>> entries = _leaderCloudMakefiles [_partition].FindMatchingEntries (eventName);

            if (entries.Count > 1)
                throw new Exception ("It is impossible for a FAIL_NODE event to have more than one corresponding DFA.");
            if (entries.Count == 1) {
                _dependencyStructure.AddEntry (entries [0]);
                _dependencyStructure.AddTokenToEntry (entries [0].Item1);
            }
#if DEBUG
            CloudManager.WriteLine (_debugFilename, "Fail Node Event: " + eventName);
#endif
        }

        private void StoppedProc (string memberName, string procName)
        {
            string eventName = memberName + "/stopped_proc";
            List<Tuple<string,int>> entries = _leaderCloudMakefiles [_partition].FindMatchingEntries (eventName);

            if (entries.Count > 1)
                throw new Exception ("It is impossible for a STOPPED_PROC event to have more than one corresponding " +
                "DFA.");
            if (entries.Count == 1) {
                _dependencyStructure.AddEntry (entries [0]);
                _dependencyStructure.AddTokenToEntry (entries [0].Item1);
            }
        }

        private void UpdateState (string filename, string xmlString)
        {
            List<string> filepathList = new List<string> (filename.Split ('/'));
            string filepath = "";
            XmlDocument xmlDoc = new XmlDocument ();
            List<Tuple<string,int>> entries;

            for (int i = 0; i < filepathList.Count - 1; i++)
                filepath += filepathList [i] + Path.DirectorySeparatorChar.ToString ();
            filepath += filepathList [filepathList.Count - 1];
            filepathList.RemoveAt (filepathList.Count - 1);
            EnsureDir (filepathList);
            xmlDoc.LoadXml (xmlString);
            xmlDoc.Save (filepath);
#if DEBUG
            CloudManager.WriteLine (_debugFilename, "Updated State of " + filepath + " to " + xmlString);
#endif
            entries = _leaderCloudMakefiles [_partition].FindMatchingEntries (filepath);
            _dependencyStructure.AddEntries (entries);
            foreach (Tuple<string,int> entry in entries)
                _dependencyStructure.AddTokenToEntry (entry.Item1);
        }

        private Tuple<HashSet<string>, List<RemoteProcessInfo>>
			ProcessCloudMakeConfigFiles (Vsync.Group vsyncGroup, HashSet<string> cloudMakeConfigFiles)
        {
            List<RemoteProcessInfo> remoteProcesses = new List<RemoteProcessInfo> ();
            List<string> statePathList = new List<string> ();
            List<string> stateNameList = new List<string> ();
            Dictionary<int,HashSet<string>> configFiles = new Dictionary<int, HashSet<string>> ();

            foreach (string cloudMakeConfigFile in cloudMakeConfigFiles) {
                XmlDocument xmlDoc = new XmlDocument ();
                XmlNode xmlNode;
                string nodename = cloudMakeConfigFile.Substring (0, cloudMakeConfigFile.IndexOf ('/'));

                xmlDoc.Load (cloudMakeConfigFile);
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
                            remoteProcesses.Add (new RemoteProcessInfo (Action.RUN, nodename, name, path, exec, args));
                        } else if (childNode.Attributes ["action"].Value == "Kill") {
                            string name = childNode ["name"].InnerText;
#if DEBUG
                            CloudManager.WriteLine (_debugFilename, "Kill process " + name + ".");
#endif
                            remoteProcesses.Add (new RemoteProcessInfo (Action.KILL, nodename, name));
                        }
                        break;
                    case ("stateFile"):
                        string statePath = childNode ["path"].InnerText;
                        string stateName = childNode ["name"].InnerText;

                        statePathList.Add (statePath);
                        stateNameList.Add (stateName);
                        break;
                    case ("configFile"):
                        string configPath = childNode ["path"].InnerText;
                        string configName = childNode ["name"].InnerText;
                        string configFilename = (configPath == "") ? nodename + "/" + configName : nodename + "/" +
                                                configPath + "/" + configName;

                        for (int i = 0; i < _leaderCloudMakefiles.Count; i++) {
                            if (_leaderCloudMakefiles [i].Compatible (configFilename).Count > 0) {
                                if (!configFiles.ContainsKey (i))
                                    configFiles [i] = new HashSet<string> ();
                                configFiles [i].Add (configFilename);
                            }
                        }

                        if ((_localCloudMakefiles.ContainsKey (nodename)) &&
                            (_localCloudMakefiles [nodename].Compatible (configFilename).Count > 0)) {
                            List<string> parameters = new List<string> ();

                            parameters.Add (configFilename);
                            vsyncGroup.P2PSend (new Address (_nameToAddress [nodename]), CloudManager.NEW_CONFIGS,
                                parameters);
                        }
                        break;
                    default:
#if DEBUG
                        CloudManager.WriteLine (_debugFilename,
                            "CloudMakeLeader: Operation " + childNode.Name + " is not supported from CloudMakefile.");
#endif
                        break;
                    }
                }

                foreach (int i in configFiles.Keys) {
                    if (i == _partition) {
                        foreach (string configFilename in configFiles[i])
                            NewConfig (configFilename);
                    } else {
                        vsyncGroup.P2PSend (new Address (_leaders [i] [0]), CloudManager.NEW_CONFIGS,
                            new List<string> (configFiles [i]));
                    }
                }

                if (statePathList.Count > 0)
                    vsyncGroup.P2PSend (new Address (_nameToAddress [nodename]), CloudManager.ASK_STATES, statePathList,
                        stateNameList);
            }

            if (configFiles.ContainsKey (_partition))
                return new Tuple<HashSet<string>, List<RemoteProcessInfo>> (configFiles [_partition], remoteProcesses);
            return new Tuple<HashSet<string>, List<RemoteProcessInfo>> (new HashSet<string> (), remoteProcesses);
        }

        private void Dist (Vsync.Group vsyncGroup, HashSet<string> activeConfigFiles,
                           HashSet<string> activeCloudMakeConfigFiles)
        {
            Tuple<HashSet<string>, List<RemoteProcessInfo>> processedInfo =
                ProcessCloudMakeConfigFiles (vsyncGroup, activeCloudMakeConfigFiles);
            HashSet<string> additionalActiveConfigFiles = new HashSet<string> ();
            List<RemoteProcessInfo> remoteProcesses = processedInfo.Item2;

            foreach (string activeConfigFile in processedInfo.Item1)
                if (File.Exists (activeConfigFile))
                    additionalActiveConfigFiles.Add (activeConfigFile);
#if DEBUG
            CloudManager.WriteLine (_debugFilename, "Active config files:");
            foreach (string filename in activeConfigFiles)
                CloudManager.WriteLine (_debugFilename, filename);
            CloudManager.WriteLine (_debugFilename, "");

            CloudManager.WriteLine (_debugFilename, "Active CloudMake config files:");
            foreach (string filename in activeCloudMakeConfigFiles)
                CloudManager.WriteLine (_debugFilename, filename);
            CloudManager.WriteLine (_debugFilename, "");
#endif
            activeConfigFiles.UnionWith (additionalActiveConfigFiles);
#if DEBUG
            CloudManager.WriteLine (_debugFilename, "Files to Distribute:");
            foreach (string filename in activeConfigFiles)
                CloudManager.WriteLine (_debugFilename, filename);
            CloudManager.WriteLine (_debugFilename, "");
#endif
            foreach (string activeConfigFile in activeConfigFiles) {
                List<string> fields = new List<string> (activeConfigFile.Split ('/'));
                string nodename = fields [0];
                List<string> fieldsList = fields.GetRange (1, fields.Count - 1);
                string path = String.Join ("/", fieldsList);
                XmlDocument xmlDoc = new XmlDocument ();
                string filename = String.Join (Path.DirectorySeparatorChar.ToString (), fields);
                string content;
                List<string> pathList = new List<string> ();
                List<string> contentList = new List<string> ();

                xmlDoc.Load (filename);
                content = xmlDoc.OuterXml;
#if DEBUG
                CloudManager.WriteLine (_debugFilename, "Distribute the content of " + filename + " to " + nodename);
#endif
                pathList.Add (path);
                contentList.Add (content);
                if (_nameToAddress.ContainsKey (nodename))
                    vsyncGroup.P2PSend (new Address (_nameToAddress [nodename]), CloudManager.UPDATE_CONFIGS, pathList,
                        contentList);
            }

            foreach (RemoteProcessInfo proc in remoteProcesses) {
                List<string> nameList = new List<string> ();
                List<string> pathList = new List<string> ();
                List<string> execList = new List<string> ();
                List<string> argsList = new List<string> ();
                string nodename;

                if (proc.GetAction () == Action.RUN) {
                    nameList.Add (proc.GetName ());
                    pathList.Add (proc.GetPath ());
                    execList.Add (proc.GetExec ());
                    argsList.Add (proc.GetArgs ());
                    nodename = proc.GetNodename ();
                    if (_nameToAddress.ContainsKey (nodename))
                        vsyncGroup.P2PSend (new Address (_nameToAddress [nodename]), CloudManager.RUN_PROCESSES,
                            nameList, pathList, execList, argsList);
                } else {
                    nameList.Add (proc.GetName ());
                    nodename = proc.GetNodename ();
                    if (_nameToAddress.ContainsKey (nodename))
                        vsyncGroup.P2PSend (new Address (_nameToAddress [nodename]), CloudManager.KILL_PROCESSES,
                            nameList);
                }
            }
        }

        private void RunCloudMake (Vsync.Group vsyncGroup)
        {
            _dependencyStructure.Run ();
            if (_rank == 0)
                Dist (vsyncGroup, _dependencyStructure.GetModifiedConfigFiles (),
                    _dependencyStructure.GetModifiedCloudMakeConfigFiles ());
            else
                vsyncGroup.P2PSend (new Address (_leaders [_partition] [_rank - 1]), CloudManager.RUN_CLOUD_MAKE);
            _dependencyStructure.Reset ();
        }

        private Tuple<List<string>,List<string>,List<string>> GetState ()
        {
            List<string> curDirs = new List<string> ();
            string leaderDir = Directory.GetCurrentDirectory ();
            List<string> resDir = new List<string> ();
            List<string> resFile = new List<string> ();
            List<string> resCont = new List<string> ();

            CloudManager.WriteLine (_debugFilename, "Leader Directory: " + leaderDir);
            curDirs.Add (leaderDir);
            while (curDirs.Count > 0) {
                string curDir = curDirs [0];
                string[] filenames = Directory.GetFiles (curDir, "*.xml");

                CloudManager.WriteLine (_debugFilename, "Examine directory: " + curDir);
                curDirs.AddRange (Directory.GetDirectories (curDir));
                resDir.AddRange (Directory.GetDirectories (curDir));
                foreach (string file in filenames) {
                    string relFilename = file.Substring (leaderDir.Length + 1);
                    XmlDocument doc = new XmlDocument ();

                    CloudManager.WriteLine (_debugFilename, "Relative Filename: " + relFilename);
                    doc.Load (file);
                    resFile.Add (relFilename);
                    resCont.Add (doc.OuterXml);
                }
                curDirs.RemoveAt (0);
            }

            for (int i = 0; i < resDir.Count; i++) {
                resDir [i] = resDir [i].Substring (leaderDir.Length + 1);
                CloudManager.WriteLine (_debugFilename, "Relative Directory: " + resDir [i]);
            }

            return new Tuple<List<string>, List<string>, List<string>> (resDir, resFile, resCont);
        }

        private void  ProcessMessages (Vsync.Group vsyncGroup, List<Tuple<MessageType, List<string>>> msgs)
        {
            foreach (Tuple<MessageType,List<string>> msg  in msgs) {
                MessageType type = msg.Item1;
                MessageType tempType;
                List<string> parameters = msg.Item2;
                List<string> tempParameters = null;
                string vsyncAddress, memberName, filename, xmlString, procName, configFilename;
                int minPartition, partition, last;

                WriteMessage (type, parameters);
                switch (type) {
                case MessageType.NEW_NODE:
                    vsyncAddress = parameters [0];
                    CloudManager.WriteLine (_debugFilename, vsyncAddress.ToString ());
                    vsyncGroup.P2PSend (new Address (vsyncAddress), CloudManager.LEADER_INFO, _vsyncAddress,
                        _partition, _rank);
                    break;
                case MessageType.FAIL_NODE:
                    vsyncAddress = parameters [0];
                    partition = CloudManager.FindPartition (_leaders, _reserveLeaders, vsyncAddress);
                    if (partition >= 0) {
                        throw new NotImplementedException ();
                    } else if (partition == -1) {
                        _reserveLeaders.Remove (vsyncAddress);
                    } else if (_addressToName.ContainsKey (vsyncAddress)) {
                        memberName = _addressToName [vsyncAddress];
                        _addressToName.Remove (vsyncAddress);
                        _nameToAddress.Remove (memberName);
                        if ((_state == State.ACTIVE) && IsEventInPartition (memberName + "/fail_node")) {
                            if (_rank == 0) {
                                if (_leaders [_partition].Count == 1) {
                                    MemberLeave (memberName);
                                    _updatesDelivered += 1;
                                } else {
                                    _pending.Add (msg);
                                    vsyncGroup.P2PSend (new Address (_leaders [_partition] [_rank + 1]),
                                        CloudManager.MEMBER_LEAVE, memberName);
                                }
                            }
                        }
                    }
                    break;
                case MessageType.MEMBER_JOIN:
                    vsyncAddress = parameters [0];
                    memberName = parameters [1];
                    _addressToName.Add (vsyncAddress, memberName);
                    _nameToAddress.Add (memberName, vsyncAddress);
                    if ((_partition >= 0) && (_rank == 0) && (_rank == _leaders [_partition].Count - 1)) {
                        NewMember (memberName);
                        _updatesDelivered += 1;
                    } else if ((_partition >= 0) && (_rank == 0)) {
                        tempType = MessageType.NEW_MEMBER;
                        tempParameters = new List<string> ();
                        tempParameters.Add (memberName);
                        _pending.Add (new Tuple<MessageType, List<string>> (tempType, tempParameters));
                        vsyncGroup.P2PSend (new Address (_leaders [_partition] [_rank + 1]), CloudManager.NEW_MEMBER,
                            memberName);
                    }
                    break;
                case MessageType.MEMBER_LEAVE:
                    if (!(_state == State.ACTIVE))
                        throw new Exception ("CloudMakeLeader: Received MEMBER_LEAVE without being active.");
                    memberName = parameters [0];
                    if (_rank == _leaders [_partition].Count - 1) {
                        MemberLeave (memberName);
                        _updatesDelivered += 1;
                        if (_rank > 0)
                            vsyncGroup.P2PSend (new Address (_leaders [_partition] [_rank - 1]), CloudManager.ACK);
                    } else {
                        _pending.Add (msg);
                        vsyncGroup.P2PSend (new Address (_leaders [_partition] [_rank + 1]), CloudManager.MEMBER_LEAVE,
                            memberName);
                    }
                    break;
                case MessageType.LEADER_JOIN:
                    vsyncAddress = parameters [0];
                    minPartition = CloudManager.FindMinPartition (_leaders);
                    if ((minPartition >= 0) && (_leaders [minPartition].Count < _nreplicas)) {
                        last = _leaders [minPartition].Count - 1;
                        _leaders [minPartition].Add (vsyncAddress);
                        CloudManager.WriteLine (_debugFilename, "Previous Leader: " + _leaders [minPartition] [last]);
                        CloudManager.WriteLine (_debugFilename, "Me: " + _vsyncAddress);
                        if ((_state == State.ACTIVE) && (_leaders [minPartition] [last] == _vsyncAddress)) {
                            Tuple<List<string>, List<string>, List<string>> triple = GetState ();

                            vsyncGroup.P2PSend (new Address (_leaders [minPartition] [last + 1]),
                                CloudManager.STATE_TRANSFER, triple.Item1, triple.Item2, triple.Item3);
                        }
                    } else {
                        _reserveLeaders.Add (vsyncAddress);
                    }
                    break;
                case MessageType.NEW_MEMBER:
                    memberName = parameters [0];
                    CloudManager.WriteLine (_debugFilename, "Partition: " + _partition.ToString ()); 
                    if (_rank == _leaders [_partition].Count - 1) {
                        CloudManager.WriteLine (_debugFilename, "Call NewMember."); 
                        NewMember (memberName);
                        _updatesDelivered += 1;
                        if (_rank > 0)
                            vsyncGroup.P2PSend (new Address (_leaders [_partition] [_rank - 1]), CloudManager.ACK);
                    } else {
                        _pending.Add (msg);
                        vsyncGroup.P2PSend (new Address (_leaders [_partition] [_rank + 1]), CloudManager.NEW_MEMBER,
                            memberName);
                    }
                    break;
                case MessageType.NEW_CONFIG:
                    configFilename = parameters [0];
                    CloudManager.WriteLine (_debugFilename, "Partition: " + _partition.ToString ()); 
                    if (_rank == _leaders [_partition].Count - 1) {
                        CloudManager.WriteLine (_debugFilename, "Call NewConfig.");
                        NewConfig (configFilename);
                        CloudManager.WriteLine (_debugFilename, "Finished NewConfig.");
                        _updatesDelivered += 1;
                        if (_rank > 0)
                            vsyncGroup.P2PSend (new Address (_leaders [_partition] [_rank - 1]), CloudManager.ACK);
                    } else {
                        _pending.Add (msg);
                        vsyncGroup.P2PSend (new Address (_leaders [_partition] [_rank + 1]), CloudManager.NEW_CONFIGS,
                            parameters);
                    }
                    break;
                case MessageType.STOPPED_PROC:
                    if (_state != State.ACTIVE)
                        throw new Exception ("CloudMakeLeader: Received STOPPED_PROC without being active.");
                    memberName = parameters [0];
                    procName = parameters [1];
                    if (_rank == _leaders [_partition].Count - 1) {
                        StoppedProc (memberName, procName);
                        _updatesDelivered += 1;
                        if (_rank > 0)
                            vsyncGroup.P2PSend (new Address (_leaders [_partition] [_rank - 1]), CloudManager.ACK);
                    } else {
                        _pending.Add (msg);
                        vsyncGroup.P2PSend (new Address (_leaders [_partition] [_rank + 1]), CloudManager.STOPPED_PROC,
                            memberName, procName);
                    }
                    break;
                case MessageType.UPDATE_STATE:
                    if (_state != State.ACTIVE)
                        throw new Exception ("CloudMakeLeader: Received UPDATE_STATE without being active");
                    filename = parameters [0];
                    xmlString = parameters [1];
                    if (_rank == _leaders [_partition].Count - 1) {
                        UpdateState (filename, xmlString);
                        _updatesDelivered += 1;
                        if (_rank > 0)
                            vsyncGroup.P2PSend (new Address (_leaders [_partition] [_rank - 1]), CloudManager.ACK);
                    } else {
                        _pending.Add (msg);
                        vsyncGroup.P2PSend (new Address (_leaders [_partition] [_rank + 1]), CloudManager.UPDATE_STATE,
                            filename, xmlString);
                    }
                    break;
                case MessageType.ACK:
                    tempType = _pending [0].Item1;
                    tempParameters = _pending [0].Item2;
                    switch (tempType) {
                    case MessageType.NEW_MEMBER:
                        CloudManager.WriteLine (_debugFilename, "Call NewMember.");
                        memberName = tempParameters [0];
                        NewMember (memberName);
                        break;
                    case MessageType.NEW_CONFIG:
                        configFilename = tempParameters [0];
                        CloudManager.WriteLine (_debugFilename, "Call NewConfig.");
                        NewConfig (configFilename);
                        CloudManager.WriteLine (_debugFilename, "Finished NewConfig.");
                        break;
                    case MessageType.MEMBER_LEAVE:
                        memberName = tempParameters [0];
                        MemberLeave (memberName);
                        break;
                    case MessageType.STOPPED_PROC:
                        memberName = tempParameters [0];
                        procName = tempParameters [1];
                        StoppedProc (memberName, procName);
                        break;
                    case MessageType.UPDATE_STATE:
                        filename = tempParameters [0];
                        xmlString = tempParameters [1];
                        UpdateState (filename, xmlString);
                        break;
                    default:
                        throw new Exception ("CloudMakeLeader: Unknown Message Type " + _pending [0].Item1.ToString () +
                        " in pending queue.");
                    }
                    _updatesDelivered += 1;
                    _pending.RemoveAt (0);
                    if (_rank > 0)
                        vsyncGroup.P2PSend (new Address (_leaders [_partition] [_rank - 1]), CloudManager.ACK);
                    break;
                case MessageType.RUN_CLOUD_MAKE:
                    RunCloudMake (vsyncGroup);
                    break;
                case MessageType.STATE_TRANSFER:
                    if (_state != State.WAITFORSTATE)
                        throw new Exception ("CloudMakeLeader: Received STATE_TRANSFER without being at WAITFORSTATE " +
                        "state");

                    List<string> directories = new List<string> ();
                    List<string> xmlFilenames = new List<string> ();
                    List<string> xmlContents = new List<string> ();

                    int i = 0;
                    while ((parameters.Count > 2 * i + 1) && (parameters [2 * i + 1] == "")) {
                        string dir = parameters [2 * i];

                        Directory.CreateDirectory (dir);
                        directories.Add (dir);
#if DEBUG
                        CloudManager.WriteLine (_debugFilename, "State Transfer Directory: " + dir);
#endif
                        i++;
                    }
                    while (parameters.Count > 2 * i + 1) {
                        string xmlFilename = parameters [2 * i];
                        string xmlContent = parameters [2 * i + 1];
                        XmlDocument xmlDoc = new XmlDocument ();

                        xmlFilenames.Add (xmlFilename);
                        xmlContents.Add (xmlContent);
#if DEBUG
                        CloudManager.WriteLine (_debugFilename, "State Transfer Filename: " + xmlFilename);
#endif
                        xmlDoc.LoadXml (xmlContent);
                        xmlDoc.Save (xmlFilename);
                        i++;
                    }
                    _state = State.ACTIVE;
                    if (_rank != _leaders [_partition].Count - 1) {
                        vsyncGroup.P2PSend (new Address (_leaders [_partition] [_rank + 1]),
                            CloudManager.STATE_TRANSFER, directories, xmlFilenames, xmlContents);
                    }
                    break;
                case MessageType.SEND_PENDING:
                    throw new NotImplementedException ();
                default:
                    throw new Exception ("CloudMakeLeader: I should not have received " + msg.ToString () +
                    " message.");
                }
                CloudManager.WriteLine (_debugFilename, "Pending:");
                foreach (Tuple<MessageType,List<string>> tempMsg in _pending)
                    WriteMessage (tempMsg.Item1, tempMsg.Item2);
            }
        }

        public int GetRank ()
        {
            return _rank;
        }

        public void Run (Vsync.Group vsyncGroup)
        {
            List<Tuple<MessageType, List<string>>> msgs;

            _dependencyStructure = _partition >= 0 ?
                new DependencyStructure (_leaderCloudMakefiles [_partition], _debugFilename) : null;
            if ((_partition >= 0) && (_rank == 0))
                FindInitialEntries ();
            while (true) {
                lock (_queueLock) {
                    while (_queue.Count == 0)
                        Monitor.Wait (_queueLock);
                    msgs = new List<Tuple<MessageType, List<string>>> (_queue);
                    _queue.Clear ();
                }
                ProcessMessages (vsyncGroup, msgs);
                if ((_state == State.ACTIVE) && (_partition >= 0) && (_rank == _leaders [_partition].Count - 1))
                    RunCloudMake (vsyncGroup);
            }
        }
    }
}