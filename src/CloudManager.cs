using CommandLine;
using Mono.Posix;
using Mono.Unix;
using Mono.Unix.Native;
using System;
using System.CodeDom;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Remoting.Messaging;
using System.Security.Cryptography;
using System.Security.Policy;
using System.Text;
using System.Threading;
using System.Xml;
using System.Xml.Schema;
using Vsync;
using State = System.UInt32;

namespace CloudManager
{
    /**
	 * Arguments needed for sending membership information to new members.
	 */
	delegate void memberInfoArgs (string vsyncAddress, string memberName);
    /**
	 * Arguments needed for sending leader information to new members and leaders.
	 */
	delegate void leaderInfoArgs (string vsyncAddress, int partition, int rank);
    /**
	 * Arguments needed for joining the members group.
	 */
	delegate void memberJoinArgs (string vsyncAddress, string memberName);
    /**
	 * Arguments needed for leaving the members group.
	 */
	delegate void memberLeaveArgs (string vsyncAddress);
    /**
	 * Arguments needed for joining the leaders group.
	 */
    delegate void leaderJoinArgs (string vsyncAddress);
    /**
     * Arguments needed for new members.
     */
    delegate void newMemberArgs (string memberName);
    /**
     * Arguments needed for registering new configurations.
     */
    delegate void newConfigsArgs (List<string> configFiles);
    /**
	 * Arguments needed for updating the failure information in CloudManager leader when an application
	 * process in a local node fails.
	 */
	delegate void stoppedProcArgs (string memberName, string procName);
    /*
	 * Arguments for updating state files.
	 */
    delegate void updateStateArgs (string filename, string xmlString);
    /*
	 * Arguments for acknowledgments.
	 */
    delegate void ackArgs ();
    /*
	 * Arguments for running CloudManager.
	 */
    delegate void runCloudMakeArgs ();
    /**
	 * Arguments to enter the group.
	 */
	delegate void enterGroupArgs (Address addr, string name);
    /**
	 * Arguments needed to run a process.
	 */
	delegate void runProcessesArgs (List<string> procNames, List<string> paths, List<string> execs, List<string> args);
    /**
	 * Arguments needed to kill a process.
	 */
	delegate void killProcessesArgs (List<string> procNames);
    /**
	 * Arguments needed for updating the status file of the CloudManager leader from a local node.
	 */
	delegate void updateStatesArgs (string nodename, List<string> names, List<string> msgs);
    /**
	 * Arguments needed for updating the configuration file of a local node from CloudManager leader.
	 */
	delegate void updateConfigsArgs (List<string> filenames, List<string> content);
    delegate void askStatesArgs (List<string> statePathList, List<string> stateNameList);
    /**
	 * Arguments needed for updating the configuration file of a local node from CloudManager leader.
	 */
	delegate void commitConfigArgs (int rank, long logicalClock);
    /**
	 * Arguments needed for killing a process of a local node from CloudManager leader.
	 */
	delegate void killArgs (string procId);
    /*
     * Arguments needed for transfering state.
     */
    delegate void stateTransferArgs (List<string> directories, List<string> filenames, List<string> contents);

    public class StatePrinter
    {
        private int _timeout;

        public StatePrinter (int timeout)
        {
            _timeout = timeout;
        }

        public void Run ()
        {
            using (System.IO.StreamWriter file = new System.IO.StreamWriter (@"Vsync_state.txt")) {
                while (true) {
                    file.WriteLine (VsyncSystem.GetState ());
                    Thread.Sleep (_timeout);
                }
            }
        }
    }

    /**
	 * Class that constructs the CloudManager tool.
	 */
    public class CloudManager
    {
        // Setup parameters.
        private string _hostname;
        private bool _isLeader;
        private int _timeout;
        private int _nreplicas;
        // Current state of CloudManager.
        private State _state;
        // Membership and Leadership paramenters.
        private HashSet<string> _waitMembers;
        private List<string> _leadersToAdd;
        private Dictionary<int, Dictionary<int, string>> _leaderRanks;
        private Dictionary<string, string> _nameToAddress;
        private Dictionary<string, string> _addressToName;
        private List<List<string>> _leaders;
        private List<string> _reserveLeaders;
        // Shared state with Local Daemon.
        private CloudManagerLocal _cloudManagerLocal;
        private CloudMakefile _localCloudMakefile;
        private Dictionary<string,CloudMakefile> _localCloudMakefiles;
        // Shared state with Leader.
        private CloudManagerLeader _cloudManagerLeader;
        private List<CloudMakefile> _leaderCloudMakefiles;
        // Debug information.
        private string _debugFilename;
        private string _localDebugFilename;
        private string _monitorDebugFilename;
        private string _leaderDebugFilename;
        // Message queue.
        private List<Tuple<MessageType, List<string>>> _queue;
        // Locks.
        private object _queueLock;
        // Message types for Vsync.
        public static int MEMBER_INFO = 0;
        public static int LEADER_INFO = 1;
        public static int MEMBER_JOIN = 2;
        public static int MEMBER_LEAVE = 3;
        public static int LEADER_JOIN = 4;
        public static int NEW_MEMBER = 5;
        public static int NEW_CONFIGS = 6;
        public static int STOPPED_PROC = 7;
        public static int UPDATE_STATE = 8;
        public static int ACK = 9;
        public static int RUN_CLOUD_MAKE = 10;
        public static int STATE_TRANSFER = 11;
        public static int SEND_PENDING = 12;
        public static int RUN_PROCESSES = 13;
        public static int KILL_PROCESSES = 14;
        public static int UPDATE_CONFIGS = 15;
        public static int ASK_STATES = 16;

        public enum State
        {
            INITIAL = 0,
            ACTIVE = 1,
        }

        public enum MessageType
        {
            NEW_NODES = 0,
            FAILED_NODES = 1,
            MEMBER_INFO = 2,
            LEADER_INFO = 3,
            MEMBER_JOIN = 4,
            MEMBER_LEAVE = 5,
            LEADER_JOIN = 6,
            NEW_MEMBER = 7,
            NEW_CONFIGS = 8,
            STOPPED_PROC = 9,
            UPDATE_STATE = 10,
            ACK = 11,
            RUN_CLOUD_MAKE = 12,
            STATE_TRANSFER = 13,
            SEND_PENDING = 14,
            RUN_PROCESSES = 15,
            KILL_PROCESSES = 16,
            UPDATE_CONFIGS = 17,
            ASK_STATES = 18,
        }

        public CloudManager (string hostname, string parseTree, bool isLeader, int timeout, int nreplicas,
                             bool isDeterministic, string debugFilename, string localDebugFilename,
                             string monitorDebugFilename, string leaderDebugFilename)
        {
            CloudMakefile cloudMakefile = new CloudMakefile ();
            List<CloudMakefile> cloudMakefiles;

            cloudMakefile.ParseMakefileTree (parseTree);
            cloudMakefile.DetectCycles ();
            cloudMakefiles = CloudMakefile.SplitCloudMakefile (cloudMakefile);
            _leaderCloudMakefiles = new List<CloudMakefile> ();
            foreach (CloudMakefile tempCloudMakefile in cloudMakefiles)
                if (!tempCloudMakefile.IsLocalCloudMakefile ())
                    _leaderCloudMakefiles.Add (tempCloudMakefile);
            cloudMakefiles = cloudMakefiles.Except (_leaderCloudMakefiles).ToList ();
            _localCloudMakefiles = new Dictionary<string, CloudMakefile> ();
            foreach (CloudMakefile tempCloudMakefile in cloudMakefiles) {
                string nodename = tempCloudMakefile.GetNode ();

                if (_localCloudMakefiles.ContainsKey (nodename))
                    throw new Exception ("CloudManager: There are more than one local CloudMakefiles for " + nodename);
                _localCloudMakefiles [nodename] = tempCloudMakefile;
            }
            _localCloudMakefile = _localCloudMakefiles.ContainsKey (hostname) ? _localCloudMakefiles [hostname] : null;
//            foreach (string nodename in _localCloudMakefiles.Keys)
//                Console.Out.WriteLine ("Local CloudMakefile: " + nodename);
            _hostname = hostname;
            _isLeader = isLeader;
            _timeout = timeout;
            _nreplicas = nreplicas;
            _state = State.INITIAL;
            _waitMembers = new HashSet<string> ();
            _leadersToAdd = new List<string> ();
            _leaderRanks = new Dictionary<int, Dictionary<int, string>> ();
            _nameToAddress = new Dictionary<string, string> ();
            _addressToName = new Dictionary<string, string> ();
            _leaders = new List<List<string>> ();
            _reserveLeaders = new List<string> ();
            for (int i = 0; i < _leaderCloudMakefiles.Count; i++)
                _leaders.Add (new List<string> ());
            _debugFilename = debugFilename;
            _localDebugFilename = localDebugFilename;
            _monitorDebugFilename = monitorDebugFilename;
            _leaderDebugFilename = leaderDebugFilename;
            _queue = new List<Tuple<MessageType, List<string>>> ();
            _queueLock = new object ();
        }

        static public string GetMyIpAddress (string debugFilename)
        {
            IPHostEntry host;
            string localIP = "?";
            bool localIPFound = false;
            bool networkInterfaceOk = false;

            while (!localIPFound) {
                try {
                    host = Dns.GetHostEntry (Dns.GetHostName ());
                    foreach (IPAddress ip in host.AddressList) {
                        WriteLine (debugFilename, "Trying " + ip.ToString ());
                        if (ip.AddressFamily == AddressFamily.InterNetwork) {
                            localIP = ip.ToString ();
                            localIPFound = true;
                            break;
                        }
                    }
                    if (!localIPFound)
                        Thread.Sleep (1000);
                } catch (Exception) {
                    WriteLine (debugFilename, "Unable to resolve my IP Address. Trying again ...");
                    Thread.Sleep (1000);
                }
            }
            WriteLine (debugFilename, "");

            while (!networkInterfaceOk) {
                WriteLine (debugFilename, "Trying Network Interfaces");
                foreach (NetworkInterface netInterface in NetworkInterface.GetAllNetworkInterfaces()) {
                    WriteLine (debugFilename, "Name: " + netInterface.Name);
                    WriteLine (debugFilename, "Description: " + netInterface.Description);
                    WriteLine (debugFilename, "Addresses: ");
                    IPInterfaceProperties ipProps = netInterface.GetIPProperties ();
                    foreach (UnicastIPAddressInformation addr in ipProps.UnicastAddresses) {
                        WriteLine (debugFilename, " " + addr.Address.ToString ());
                        if (localIP.Equals (addr.Address.ToString ())) {
                            networkInterfaceOk = true;
                            break;
                        }
                    }
                    if (networkInterfaceOk)
                        break;
                    WriteLine (debugFilename, "");
                }
                if (!networkInterfaceOk) {
                    WriteLine (debugFilename, "Unable to obtain the correct network interface. Trying again ...");
                    Thread.Sleep (1000);
                }
            }
            WriteLine (debugFilename, "");

            return localIP;
        }

        static public string GetIpAddress (string hostname, string debugFilename)
        {
            IPHostEntry host;
            string IP = "?";
            bool IPFound = false;

            while (!IPFound) {
                try {
                    host = Dns.GetHostEntry (hostname);
                    foreach (IPAddress ip in host.AddressList) {
                        WriteLine (debugFilename, "Trying " + ip.ToString ());
                        if (ip.AddressFamily == AddressFamily.InterNetwork) {
                            IP = ip.ToString ();
                            IPFound = true;
                        }
                    }
                    if (!IPFound)
                        Thread.Sleep (1000);
                } catch (Exception) {
                    WriteLine (debugFilename, "Unable to resolve IP Address. Trying again ...");
                    Thread.Sleep (1000);
                }
            }
            WriteLine (debugFilename, "");

            return IP;
        }

        public static string GetXMLAsString (string file)
        {
            XmlDocument doc = new XmlDocument ();

            try {
                doc.Load (file);
            } catch (XmlException) {
                return null;
            }
            return doc.OuterXml;
        }

        public static string GetHash (string text)
        {
            byte[] bytes = GetBytes (text);
            SHA256Managed hashstring = new SHA256Managed ();
            byte[] hash = hashstring.ComputeHash (bytes);

            return GetString (hash);
        }

        private static byte[] GetBytes (string str)
        {
            byte[] bytes = new byte[str.Length * sizeof(char)];
            System.Buffer.BlockCopy (str.ToCharArray (), 0, bytes, 0, bytes.Length);
            return bytes;
        }

        private static string GetString (byte[] bytes)
        {
            char[] chars = new char[bytes.Length / sizeof(char)];
            System.Buffer.BlockCopy (bytes, 0, chars, 0, bytes.Length);
            return new string (chars);
        }

        private void WriteMessage (MessageType type, List<string> parameters)
        {
#if DEBUG
            string str = type.ToString () + ":";

            foreach (string param in parameters)
                str += " " + param;
            WriteLine (str);
#endif
        }

        public static void AddLeader (List<List<string>> leaders, List<string> reserveLeaders, int nReplicas,
                                      string vsyncAddress)
        {
            int minPartition = FindMinPartition (leaders);

            if ((minPartition >= 0) && (leaders [minPartition].Count < nReplicas))
                leaders [minPartition].Add (vsyncAddress);
            else
                reserveLeaders.Add (vsyncAddress);
        }

        public static void RemoveLeader (List<List<string>> leaders, List<string> reserveLeaders, string vsyncAddress)
        {
            for (int i = 0; i < leaders.Count; i++) {
                if (leaders [i].Contains (vsyncAddress)) {
                    leaders [i].Remove (vsyncAddress);
                    return;
                }
            }

            if (reserveLeaders.Contains (vsyncAddress))
                reserveLeaders.Remove (vsyncAddress);
        }

        private void BalanceLeaders (Vsync.Group vsyncGroup)
        {
            int minPartition = FindMinPartition ();

            WriteLine (_debugFilename, minPartition.ToString ());
            while ((_reserveLeaders.Count > 0) && (minPartition >= 0) && (_leaders [minPartition].Count < _nreplicas)) {
                _leaders [minPartition].Add (_reserveLeaders [0]);
                _reserveLeaders.RemoveAt (0);
                minPartition = FindMinPartition ();
            }
        }

        private Tuple<int,int> FindInitialPartition (string vsyncAddress)
        {
            foreach (int partition in _leaderRanks.Keys)
                foreach (int rank in _leaderRanks[partition].Keys)
                    if (_leaderRanks [partition] [rank] == vsyncAddress)
                        return new Tuple<int, int> (partition, rank);
            return new Tuple<int,int> (-2, -2);
        }

        private int FindPartition (string vsyncAddress)
        {
            return FindPartition (_leaders, _reserveLeaders, vsyncAddress);
        }

        public static int FindPartition (List<List<string>> leaders, List<string> reserveLeaders, string vsyncAddress)
        {
            for (int i = 0; i < leaders.Count; i++)
                if (leaders [i].Contains (vsyncAddress))
                    return i;
            if (reserveLeaders.Contains (vsyncAddress))
                return -1;
            return -2;
        }

        private int FindMinPartition ()
        {
            return FindMinPartition (_leaders);
        }

        public static int FindMinPartition (List<List<string>> leaders)
        {
            int minCount = Int32.MaxValue;
            int minPartition = -1;

            for (int i = 0; i < leaders.Count; i++) {
                if (leaders [i].Count < minCount) {
                    minCount = leaders [i].Count;
                    minPartition = i;
                }
            }

            return minPartition;
        }

        private void  UpdateState (Vsync.Group vsyncGroup, List<Tuple<MessageType, List<string>>> msgs)
        {
            CloudManagerLeader.MessageType leaderType;
            CloudManagerLocal.MessageType localType;
            List<string> leaderParameters, localParameters;

            foreach (Tuple<MessageType,List<string>> msg  in msgs) {
                MessageType type = msg.Item1;
                List<string> parameters = msg.Item2;
                Tuple<int,int> partitionRankTuple;
                string vsyncAddress, memberName;
                int leaderPartition = -2;
                int leaderRank = -2;

                WriteMessage (type, parameters);
                switch (type) {
                case MessageType.NEW_NODES:
                    WriteLine (_debugFilename, "NEW NODES");
                    if (_state == State.ACTIVE) {
                        foreach (string tempVsyncAddress in parameters) {
                            List<string> tempParameters = new List<string> ();

                            tempParameters.Add (tempVsyncAddress);
                            if (_isLeader) {
                                WriteLine (_debugFilename, "Leader NEW NODES");
                                _cloudManagerLeader.AddMessage (new Tuple<CloudManagerLeader.MessageType, List<string>> (
                                    CloudManagerLeader.MessageType.NEW_NODE, tempParameters));
                            } else {
                                WriteLine (_debugFilename, "Local NEW NODES");
                                _cloudManagerLocal.AddMessage (new Tuple<CloudManagerLocal.MessageType, List<string>> (
                                    CloudManagerLocal.MessageType.NEW_NODE, tempParameters));
                            }
                        }
                    }
                    break;
                case MessageType.FAILED_NODES:
                    if (_state == State.INITIAL) {
                        foreach (string tempVsyncAddress in parameters) {
                            if (_waitMembers.Contains (tempVsyncAddress)) {
                                _waitMembers.Remove (tempVsyncAddress);
                                TryJoin (vsyncGroup);
                            } else if (_addressToName.ContainsKey (tempVsyncAddress)) {
                                memberName = _addressToName [tempVsyncAddress];
                                _nameToAddress.Remove (memberName);
                                _addressToName.Remove (tempVsyncAddress);
                            } else {
                                partitionRankTuple = FindInitialPartition (tempVsyncAddress);
                                leaderPartition = partitionRankTuple.Item1;
                                leaderRank = partitionRankTuple.Item2;
                                if (leaderPartition >= -1)
                                    _leaderRanks [leaderPartition].Remove (leaderRank);
                                else if (_leadersToAdd.Contains (tempVsyncAddress))
                                    _leadersToAdd.Remove (tempVsyncAddress);
                            }
                        }
                    } else {
                        if (_isLeader) {
                            foreach (string tempVsyncAddress in parameters) {
                                List<string> tempParameters = new List<string> ();

                                tempParameters.Add (tempVsyncAddress);
                                _cloudManagerLeader.AddMessage (new Tuple<CloudManagerLeader.MessageType,
                                    List<string>> (CloudManagerLeader.MessageType.FAIL_NODE, tempParameters));
                            }
                        } else {
                            foreach (string tempVsyncAddress in parameters) {
                                List<string> tempParameters = new List<string> ();

                                tempParameters.Add (tempVsyncAddress);
                                _cloudManagerLocal.AddMessage (new Tuple<CloudManagerLocal.MessageType,
                                    List<string>> (CloudManagerLocal.MessageType.FAIL_NODE, tempParameters));
                            }
                        }
                    }
                    break;
                case MessageType.MEMBER_INFO:
                    vsyncAddress = parameters [0];
                    memberName = parameters [1];
                    if (_state == State.ACTIVE)
                        throw new Exception ("CloudManager: I should not have received MEMBER_INFO message from " +
                        vsyncAddress + " since I am ACTIVE.");
                    if (!_waitMembers.Contains (vsyncAddress))
                        throw new Exception ("CloudManager: I should not have received MEMBER_INFO message from " +
                        vsyncAddress + " since it is not in my \"waiting\" list.");
                    _waitMembers.Remove (vsyncAddress);
                    _nameToAddress.Add (memberName, vsyncAddress);
                    _addressToName.Add (vsyncAddress, memberName);
                    TryJoin (vsyncGroup);
                    break;
                case MessageType.LEADER_INFO:
                    vsyncAddress = parameters [0];
                    leaderPartition = Int32.Parse (parameters [1]);
                    leaderRank = Int32.Parse (parameters [2]);
                    if (_state == State.ACTIVE)
                        throw new Exception ("CloudManager: I should not have received LEADER_INFO message from " +
                        vsyncAddress + " since I am ACTIVE.");
                    if (!_waitMembers.Contains (vsyncAddress))
                        throw new Exception ("CloudManager: I should not have received LEADER_INFO message from " +
                        vsyncAddress + " since it is not in my \"waiting\" list.");
                    _waitMembers.Remove (vsyncAddress);
                    if (leaderPartition >= -1) {
                        if (!_leaderRanks.ContainsKey (leaderPartition))
                            _leaderRanks.Add (leaderPartition, new Dictionary<int, string> ());
                        _leaderRanks [leaderPartition].Add (leaderRank, vsyncAddress);
                    } else
                        throw new Exception ("CloudManager: Leader cannot be in partition " + leaderPartition.ToString ());
                    TryJoin (vsyncGroup);
                    break;
                case MessageType.MEMBER_JOIN:
                    vsyncAddress = parameters [0];
                    memberName = parameters [1];
                    if (_state == State.INITIAL) {
                        _nameToAddress.Add (memberName, vsyncAddress);
                        _addressToName.Add (vsyncAddress, memberName);
                        if (_waitMembers.Contains (vsyncAddress)) {
                            _waitMembers.Remove (vsyncAddress);
                            TryJoin (vsyncGroup);
                        }
                        if (memberName == _hostname) {
                            List<int> sortedPartitions = new List<int> (_leaderRanks.Keys);
                            List<int> sortedRanks;

                            sortedPartitions.Sort ();
                            if ((sortedPartitions.Count > 0) && (sortedPartitions [0] == -1)) {
                                sortedRanks = new List<int> (_leaderRanks [-1].Keys);
                                sortedRanks.Sort ();
                                for (int i = 0; i < sortedRanks.Count; i++)
                                    _reserveLeaders.Add (_leaderRanks [-1] [sortedRanks [i]]);
                                sortedPartitions.Remove (-1);
                            }
                            foreach (int partition in sortedPartitions) {
                                sortedRanks = new List<int> (_leaderRanks [partition].Keys);
                                sortedRanks.Sort ();
                                for (int i = 0; i < sortedRanks.Count; i++)
                                    _leaders [partition].Add (_leaderRanks [partition] [sortedRanks [i]]);
                            }
                            _reserveLeaders.AddRange (_leadersToAdd);
                            BalanceLeaders (vsyncGroup);
                            _state = State.ACTIVE;
                            _cloudManagerLocal = new CloudManagerLocal (_leaderCloudMakefiles, _localCloudMakefile,
                                _hostname, _leaders, _reserveLeaders, _nreplicas, _timeout, vsyncGroup,
                                _localDebugFilename, _monitorDebugFilename);
                            Thread localThread = new Thread (delegate() {
                                _cloudManagerLocal.Run (vsyncGroup);
                            });
                            localThread.Start ();
#if DEBUG
                            WriteLine ("State when Joined:");
                            WriteLine ("Members:");
                            foreach (string mname in _nameToAddress.Keys)
                                WriteLine (mname + ":" + _nameToAddress [mname]);
                            WriteLine ("Leaders:");
                            for (int partition = 0; partition < _leaders.Count; partition++) {
                                string str = "";
                                for (int rank = 0; rank < _leaders [partition].Count; rank++)
                                    str += _leaders [partition] [rank] + " ";
                                WriteLine (str);
                            }
                            WriteLine ("ReserveLeaders:");
                            foreach (string lname in _reserveLeaders)
                                WriteLine (lname);
#endif
                            if ((_localCloudMakefile != null) &&
                                (_localCloudMakefile.FindMatchingEntries (_hostname + "/new_node").Count > 0)) {
                                localType = CloudManagerLocal.MessageType.MEMBER_JOIN;
                                localParameters = new List<string> ();
                                localParameters.Add (memberName);
                                _cloudManagerLocal.AddMessage (
                                    new Tuple<CloudManagerLocal.MessageType,List<string>> (localType, localParameters));
                            }
                        }
                    } else {
                        if (_isLeader) {
                            leaderType = CloudManagerLeader.MessageType.MEMBER_JOIN;
                            leaderParameters = new List<string> ();
                            leaderParameters.Add (vsyncAddress);
                            leaderParameters.Add (memberName);
                            _cloudManagerLeader.AddMessage (new Tuple<CloudManagerLeader.MessageType,
                                List<string>> (leaderType, leaderParameters));
                        }
                    }
                    break;
                case MessageType.MEMBER_LEAVE:
                    if (!_isLeader)
                        throw new Exception ("CloudManager: Cannot have a message MEMBER_LEAVE as a member.");
                    if (_state != State.ACTIVE)
                        throw new Exception ("CloudManager: Cannot receive MEMBER_LEAVE while inactive.");
                    memberName = parameters [0];
                    _cloudManagerLeader.AddMessage (new Tuple<CloudManagerLeader.MessageType,
                        List<string>> (CloudManagerLeader.MessageType.MEMBER_LEAVE, parameters));
                    break;
                case MessageType.LEADER_JOIN:
                    vsyncAddress = parameters [0];
                    if (_state == State.INITIAL) {
                        _leadersToAdd.Add (vsyncAddress);
                        if (_waitMembers.Contains (vsyncAddress)) {
                            _waitMembers.Remove (vsyncAddress);
                            TryJoin (vsyncGroup);
                        }
                        if (vsyncAddress == VsyncSystem.GetMyAddress ().ToStringVerboseFormat ()) {
                            List<int> sortedPartitions = new List<int> (_leaderRanks.Keys);
                            List<int> sortedRanks;

                            sortedPartitions.Sort ();
                            if ((sortedPartitions.Count > 0) && (sortedPartitions [0] == -1)) {
                                sortedRanks = new List<int> (_leaderRanks [-1].Keys);
                                sortedRanks.Sort ();
                                for (int i = 0; i < sortedRanks.Count; i++)
                                    _reserveLeaders.Add (_leaderRanks [-1] [sortedRanks [i]]);
                                sortedPartitions.Remove (-1);
                            }
                            foreach (int partition in sortedPartitions) {
                                sortedRanks = new List<int> (_leaderRanks [partition].Keys);
                                sortedRanks.Sort ();
                                for (int i = 0; i < sortedRanks.Count; i++)
                                    _leaders [partition].Add (_leaderRanks [partition] [sortedRanks [i]]);
                            }
                            _reserveLeaders.AddRange (_leadersToAdd);
                            BalanceLeaders (vsyncGroup);
                            _state = State.ACTIVE;
                            _cloudManagerLeader = new CloudManagerLeader (_leaderCloudMakefiles, _localCloudMakefiles,
                                _nameToAddress, _addressToName, _leaders, _reserveLeaders, _nreplicas, _hostname,
                                _leaderDebugFilename);
                            Thread leaderThread = new Thread (delegate() {
                                _cloudManagerLeader.Run (vsyncGroup);
                            });
                            leaderThread.Start ();
#if DEBUG
                            WriteLine ("State when Joined:");
                            WriteLine ("Members:");
                            foreach (string mname in _nameToAddress.Keys)
                                WriteLine (mname + ":" + _nameToAddress [mname]);
                            WriteLine ("Leaders:");
                            for (int partition = 0; partition < _leaders.Count; partition++) {
                                string str = "";
                                for (int rank = 0; rank < _leaders [partition].Count; rank++)
                                    str += _leaders [partition] [rank] + " ";
                                WriteLine (str);
                            }
                            WriteLine ("ReserveLeaders:");
                            foreach (string lname in _reserveLeaders)
                                WriteLine (lname);
#endif
                        }
                    } else {
                        if (_isLeader) {
                            leaderType = CloudManagerLeader.MessageType.LEADER_JOIN;
                            leaderParameters = new List<string> ();
                            leaderParameters.Add (vsyncAddress);
                            _cloudManagerLeader.AddMessage (new Tuple<CloudManagerLeader.MessageType,
                                List<string>> (leaderType, leaderParameters));
                        } else {
                            localType = CloudManagerLocal.MessageType.LEADER_JOIN;
                            localParameters = new List<string> ();
                            localParameters.Add (vsyncAddress);
                            _cloudManagerLocal.AddMessage (new Tuple<CloudManagerLocal.MessageType,
                                List<string>> (localType, localParameters));
                        }
                    }
                    break;
                case MessageType.NEW_MEMBER:
                    if (!_isLeader)
                        throw new Exception ("CloudManager: A non-leader received NEW_MEMBER message");
                    leaderType = CloudManagerLeader.MessageType.NEW_MEMBER;
                    leaderParameters = parameters;
                    _cloudManagerLeader.AddMessage (new Tuple<CloudManagerLeader.MessageType,
                        List<string>> (leaderType, leaderParameters));
                    break;
                case MessageType.NEW_CONFIGS:
                    if (_isLeader) {
                        foreach (string parameter in parameters) {
                            leaderType = CloudManagerLeader.MessageType.NEW_CONFIG;
                            leaderParameters = new List<string> ();
                            leaderParameters.Add (parameter);
                            _cloudManagerLeader.AddMessage (
                                new Tuple<CloudManagerLeader.MessageType, List<string>> (leaderType, leaderParameters));
                        }
                    } else {
                        foreach (string parameter in parameters) {
                            localType = CloudManagerLocal.MessageType.NEW_CONFIG;
                            localParameters = new List<string> ();
                            localParameters.Add (parameter);
                            _cloudManagerLocal.AddMessage (
                                new Tuple<CloudManagerLocal.MessageType, List<string>> (localType, localParameters));
                        }
                    }
                    break;
                case MessageType.STOPPED_PROC:
                    if (!_isLeader)
                        throw new Exception ("CloudManager: A non-leader received STOPPED_PROC message");
                    leaderType = CloudManagerLeader.MessageType.STOPPED_PROC;
                    leaderParameters = parameters;
                    _cloudManagerLeader.AddMessage (new Tuple<CloudManagerLeader.MessageType,
                        List<string>> (leaderType, leaderParameters));
                    break;
                case MessageType.UPDATE_STATE:
                    if (!_isLeader)
                        throw new Exception ("CloudManager: A non-leader received UPDATE_STATE message");
                    leaderType = CloudManagerLeader.MessageType.UPDATE_STATE;
                    leaderParameters = parameters;
                    _cloudManagerLeader.AddMessage (new Tuple<CloudManagerLeader.MessageType,
                        List<string>> (leaderType, leaderParameters));
                    break;
                case MessageType.ACK:
                    if (!_isLeader)
                        throw new Exception ("CloudManager: A non-leader received ACK message");
                    leaderType = CloudManagerLeader.MessageType.ACK;
                    leaderParameters = parameters;
                    _cloudManagerLeader.AddMessage (new Tuple<CloudManagerLeader.MessageType,
                        List<string>> (leaderType, leaderParameters));
                    break;
                case MessageType.RUN_CLOUD_MAKE:
                    if (!_isLeader)
                        throw new Exception ("CloudManager: A non-leader received RUN_CLOUD_MAKE message");
                    leaderType = CloudManagerLeader.MessageType.RUN_CLOUD_MAKE;
                    leaderParameters = parameters;
                    _cloudManagerLeader.AddMessage (new Tuple<CloudManagerLeader.MessageType,
                        List<string>> (leaderType, leaderParameters));
                    break;
                case MessageType.STATE_TRANSFER:
                    if (!_isLeader)
                        throw new Exception ("CloudManager: A non-leader received STATE_TRANSFER message");
                    leaderType = CloudManagerLeader.MessageType.STATE_TRANSFER;
                    leaderParameters = parameters;
                    _cloudManagerLeader.AddMessage (new Tuple<CloudManagerLeader.MessageType,
                        List<string>> (leaderType, leaderParameters));
                    break;
                case MessageType.RUN_PROCESSES:
                    if (_isLeader)
                        throw new Exception ("CloudManager: A leader received RUN_PROCESSES message");
                    localType = CloudManagerLocal.MessageType.RUN_PROCESS;
                    for (int i = 0; i < parameters.Count; i += 4) {
                        localParameters = new List<string> (parameters.GetRange (i, 4));
                        _cloudManagerLocal.AddMessage (new Tuple<CloudManagerLocal.MessageType,
                            List<string>> (localType, localParameters));
                    }
                    break;
                case MessageType.KILL_PROCESSES:
                    if (_isLeader)
                        throw new Exception ("CloudManager: A leader received KILL_PROCESSES message");
                    localType = CloudManagerLocal.MessageType.KILL_PROCESS;
                    for (int i = 0; i < parameters.Count; i++) {
                        localParameters = new List<string> (parameters.GetRange (i, 1));
                        _cloudManagerLocal.AddMessage (new Tuple<CloudManagerLocal.MessageType,
                            List<string>> (localType, localParameters));
                    }
                    break;
                case MessageType.UPDATE_CONFIGS:
                    if (_isLeader)
                        throw new Exception ("CloudManager: A leader received UPDATE_CONFIGS message");
                    localType = CloudManagerLocal.MessageType.UPDATE_CONFIG;
                    for (int i = 0; i < parameters.Count; i += 2) {
                        localParameters = new List<string> (parameters.GetRange (i, 2));
                        _cloudManagerLocal.AddMessage (new Tuple<CloudManagerLocal.MessageType,
                            List<string>> (localType, localParameters));
                    }
                    break;
                case MessageType.ASK_STATES:
                    if (_isLeader)
                        throw new Exception ("CloudManager: A leader received ASK_STATES message");
                    localType = CloudManagerLocal.MessageType.ASK_STATE;
                    for (int i = 0; i < parameters.Count; i += 2) {
                        localParameters = new List<string> (parameters.GetRange (i, 2));
                        _cloudManagerLocal.AddMessage (new Tuple<CloudManagerLocal.MessageType,
                            List<string>> (localType, localParameters));
                    }
                    break;
                default:
                    throw new Exception ("CloudManager: I should not have received " + msg.ToString () + " message.");
                }
            }

        }

        private void TryJoin (Vsync.Group vsyncGroup)
        {
            string myAddress = VsyncSystem.GetMyAddress ().ToStringVerboseFormat ();

            if (_waitMembers.Count == 0) {
                if (_isLeader)
                    vsyncGroup.SafeSend (LEADER_JOIN, myAddress);
                else
                    vsyncGroup.SafeSend (MEMBER_JOIN, myAddress, _hostname);
            }
        }

        private void Run (Vsync.Group vsyncGroup)
        {
            List<Tuple<MessageType, List<string>>> msgs;

            TryJoin (vsyncGroup);
            while (true) {
                lock (_queueLock) {
                    while (_queue.Count == 0)
                        Monitor.Wait (_queueLock);
                    msgs = new List<Tuple<MessageType, List<string>>> (_queue);
                    _queue.Clear ();
                }
                UpdateState (vsyncGroup, msgs);
            }
        }

        private void AddMessage (Tuple<MessageType, List<string>> msg)
        {
            lock (_queueLock) {
                _queue.Add (msg);
                Monitor.PulseAll (_queueLock);
            }
        }

        private void AddHandlers (Vsync.Group vsyncGroup)
        {
            vsyncGroup.ViewHandlers += (ViewHandler)delegate (View v) {
                if (v.joiners.Contains (VsyncSystem.GetMyAddress ())) {
                    HashSet<Address> vsyncAddresses = new HashSet<Address> (v.members);
#if DEBUG
                    foreach (Address addr in v.joiners)
                        WriteLine ("Node " + addr.ToStringVerboseFormat () + " joined Vsync.");
#endif
                    vsyncAddresses.ExceptWith (v.joiners);
                    foreach (Address addr in vsyncAddresses) {
                        _waitMembers.Add (addr.ToStringVerboseFormat ());
#if DEBUG
                        WriteLine ("Wait for " + addr.ToStringVerboseFormat ());
#endif
                    }
                }

                if (v.joiners.Count () > 0) {
                    Tuple<MessageType, List<string>> msg =
                        new Tuple<MessageType,List<string>> (MessageType.NEW_NODES, new List<string> ());

                    foreach (Address addr in v.joiners)
                        msg.Item2.Add (addr.ToStringVerboseFormat ());
                    AddMessage (msg);
                }

                if (v.leavers.Count () > 0) {
                    Tuple<MessageType, List<string>> msg =
                        new Tuple<MessageType,List<string>> (MessageType.FAILED_NODES, new List<string> ());
					
                    foreach (Address addr in v.leavers) {
                        msg.Item2.Add (addr.ToStringVerboseFormat ());
                    }
                    AddMessage (msg);
                }
            };

            vsyncGroup.Handlers [MEMBER_INFO] += (memberInfoArgs)delegate(string vsyncAddress, string memberName) {
                MessageType type = MessageType.MEMBER_INFO;
                List<string> parameters = new List<string> ();

                parameters.Add (vsyncAddress);
                parameters.Add (memberName);
                AddMessage (new Tuple<MessageType, List<string>> (type, parameters));
            };

            vsyncGroup.Handlers [LEADER_INFO] += (leaderInfoArgs)delegate(string vsyncAddress, int partition,
                                                                          int rank) {
                MessageType type = MessageType.LEADER_INFO;
                List<string> parameters = new List<string> ();

                parameters.Add (vsyncAddress);
                parameters.Add (partition.ToString ());
                parameters.Add (rank.ToString ());
                AddMessage (new Tuple<MessageType, List<string>> (type, parameters));
            };

            vsyncGroup.Handlers [MEMBER_JOIN] += (memberJoinArgs)delegate(string vsyncAddress, string memberName) {
                MessageType type = MessageType.MEMBER_JOIN;
                List<string> parameters = new List<string> ();

                parameters.Add (vsyncAddress);
                parameters.Add (memberName);
                AddMessage (new Tuple<MessageType, List<string>> (type, parameters));
            };

            vsyncGroup.Handlers [MEMBER_LEAVE] += (memberLeaveArgs)delegate(string memberName) {
                MessageType type = MessageType.MEMBER_LEAVE;
                List<string> parameters = new List<string> ();

                parameters.Add (memberName);
                AddMessage (new Tuple<MessageType, List<string>> (type, parameters));
            };

            vsyncGroup.Handlers [LEADER_JOIN] += (leaderJoinArgs)delegate(string vsyncAddress) {
                MessageType type = MessageType.LEADER_JOIN;
                List<string> parameters = new List<string> ();

                parameters.Add (vsyncAddress);
                AddMessage (new Tuple<MessageType, List<string>> (type, parameters));
            };

            vsyncGroup.Handlers [NEW_MEMBER] += (newMemberArgs)delegate(string memberName) {
                MessageType type = MessageType.NEW_MEMBER;
                List<string> parameters = new List<string> ();

                parameters.Add (memberName);
                AddMessage (new Tuple<MessageType, List<string>> (type, parameters));
            };

            vsyncGroup.Handlers [NEW_CONFIGS] += (newConfigsArgs)delegate(List<string> configFiles) {
                MessageType type = MessageType.NEW_CONFIGS;
                List<string> parameters = new List<string> ();

                parameters.AddRange (configFiles);
                AddMessage (new Tuple<MessageType, List<string>> (type, parameters));
            };

            vsyncGroup.Handlers [STOPPED_PROC] += (stoppedProcArgs)delegate(string memberName, string procName) {
                MessageType type = MessageType.STOPPED_PROC;
                List<string> parameters = new List<string> ();

                parameters.Add (memberName);
                parameters.Add (procName);
                AddMessage (new Tuple<MessageType, List<string>> (type, parameters));
            };

            vsyncGroup.Handlers [UPDATE_STATE] += (updateStateArgs)delegate(string filename, string xmlString) {
                MessageType type = MessageType.UPDATE_STATE;
                List<string> parameters = new List<string> ();

                parameters.Add (filename);
                parameters.Add (xmlString);
                AddMessage (new Tuple<MessageType, List<string>> (type, parameters));

            };

            vsyncGroup.Handlers [ACK] += (ackArgs)delegate() {
                MessageType type = MessageType.ACK;
                List<string> parameters = new List<string> ();

                AddMessage (new Tuple<MessageType, List<string>> (type, parameters));
            };

            vsyncGroup.Handlers [RUN_CLOUD_MAKE] += (ackArgs)delegate() {
                MessageType type = MessageType.RUN_CLOUD_MAKE;
                List<string> parameters = new List<string> ();

                AddMessage (new Tuple<MessageType, List<string>> (type, parameters));
            };

            vsyncGroup.Handlers [STATE_TRANSFER] += (stateTransferArgs)delegate(List<string> directories,
                                                                                List<string> filenames,
                                                                                List<string> contents) {
                MessageType type = MessageType.STATE_TRANSFER;
                List<string> parameters = new List<string> ();

                for (int i = 0; i < directories.Count; i++) {
                    parameters.Add (directories [i]);
                    parameters.Add ("");
                }
                for (int i = 0; i < filenames.Count; i++) {
                    parameters.Add (filenames [i]);
                    parameters.Add (contents [i]);
                }
                AddMessage (new Tuple<MessageType, List<string>> (type, parameters));
            };

            vsyncGroup.Handlers [RUN_PROCESSES] += (runProcessesArgs)delegate(List<string> procNames,
                                                                              List<string> paths, List<string> execs,
                                                                              List<string> args) {
                MessageType type = MessageType.RUN_PROCESSES;
                List<string> parameters = new List<string> ();

                for (int i = 0; i < procNames.Count; i++) {
                    parameters.Add (procNames [i]);
                    parameters.Add (paths [i]);
                    parameters.Add (execs [i]);
                    parameters.Add (args [i]);
                }
                AddMessage (new Tuple<MessageType, List<string>> (type, parameters));
            };

            vsyncGroup.Handlers [KILL_PROCESSES] += (killProcessesArgs)delegate(List<string> procNames) {
                MessageType type = MessageType.KILL_PROCESSES;
                List<string> parameters = new List<string> ();

                for (int i = 0; i < procNames.Count; i++)
                    parameters.Add (procNames [i]);
                AddMessage (new Tuple<MessageType, List<string>> (type, parameters));
            };

            vsyncGroup.Handlers [UPDATE_CONFIGS] += (updateConfigsArgs)delegate(List<string> filenames,
                                                                                List<string> contents) {
                MessageType type = MessageType.UPDATE_CONFIGS;
                List<string> parameters = new List<string> ();

                for (int i = 0; i < filenames.Count; i++) {
                    parameters.Add (filenames [i]);
                    parameters.Add (contents [i]);
                }
                AddMessage (new Tuple<MessageType, List<string>> (type, parameters));
            };

            vsyncGroup.Handlers [ASK_STATES] += (askStatesArgs)delegate(List<string> statePathList,
                                                                        List<string> stateNameList) {
                MessageType type = MessageType.ASK_STATES;
                List<string> parameters = new List<string> ();

                for (int i = 0; i < statePathList.Count; i++) {
                    parameters.Add (statePathList [i]);
                    parameters.Add (stateNameList [i]);
                }
                AddMessage (new Tuple<MessageType, List<string>> (type, parameters));
            };
        }

        private Vsync.Group BeMaster (string groupname, int nworkers, string masterDebugFilename)
        {
            // Prepare Master.
            bool done = false;
            Address[] myWorkers = new Address[nworkers];
            Address[] addWorker = new Address[1];
            int count = 0;

            // Start the Vsync Group.
#if DEBUG
            WriteLine (masterDebugFilename, "Start as a master.");
#endif
            VsyncSystem.Start (true);
            Vsync.Group vsyncGroup = new Vsync.Group (groupname);

            // Start printing the state of the system.
            /*
			WriteLine ("Start printer of Vsync state.");
			statePrinterThread = new Thread (delegate() {
				StatePrinter statePrinter = new StatePrinter (100);
				statePrinter.Run ();
			}
			);
			statePrinterThread.Start ();
*/

            // Add handlers to the worker.
#if DEBUG
            WriteLine (masterDebugFilename, "Add handlers.");
#endif
            AddHandlers (vsyncGroup);

            // Get Master Address.
            string myAddr = VsyncSystem.GetMyAddress ().ToStringVerboseFormat ();
#if DEBUG
            WriteLine (masterDebugFilename, "My Vsync Address is " + myAddr);
#endif

            // Master function.
            VsyncSystem.RegisterAsMaster ((NewWorker)delegate(Address hisAddress) {
                lock (myWorkers) {
                    if (done) {
                        addWorker [0] = hisAddress;
                        /*
					Console.Out.WriteLine ("Batch Start {0}", hisAddress);
					VsyncSystem.BatchStart (addWorker);
					Console.Out.WriteLine ("Wait For Worker Setup {0}", hisAddress);
					VsyncSystem.WaitForWorkerSetup (addWorker);
					Console.Out.WriteLine ("Multi Join {0}", hisAddress);
					Vsync.Group.multiJoin(addWorker, new Vsync.Group[] { vsyncGroup });
					*/
                    } else {
#if DEBUG
                        WriteLine (masterDebugFilename,
                            "Worker's Vsync Address is " + hisAddress.ToStringVerboseFormat () + ".");
#endif
                        myWorkers [count] = hisAddress;
                        count++;
                    }
                }
            }
            );

            // Wait until you collect all workers.
            while (true) {
                lock (myWorkers) {
#if DEBUG
                    WriteLine (masterDebugFilename, "Count: " + count);
#endif
                    if (count == nworkers) {
                        done = true;
                        break;
                    }
                }
                Thread.Sleep (2000);
            }

            // Create the group.
#if DEBUG
            WriteLine (masterDebugFilename, "Create the group.");
#endif
            vsyncGroup.Join ();

            // Now we activate all the Workers simultaneously.
#if DEBUG
            WriteLine (masterDebugFilename, "Start the workers.");
#endif
            VsyncSystem.BatchStart (myWorkers);

            // This delays until they have all finished their batch start.
#if DEBUG
            WriteLine (masterDebugFilename, "Wait until all workers are finished.");
#endif
            VsyncSystem.WaitForWorkerSetup (myWorkers);

            // MultiJoin all nodes.
#if DEBUG
            WriteLine (masterDebugFilename, "Multijoin.");
#endif
            Vsync.Group.multiJoin (myWorkers, new Vsync.Group[] { vsyncGroup });
            return vsyncGroup;
        }

        private Vsync.Group BeWorker (string groupname, string master, string workerDebugFilename)
        {
            Vsync.Group vsyncGroup;

            // Form the master Vsync Address from a file.
#if DEBUG
            WriteLine (workerDebugFilename, "Run as a worker and serve master " + master + ".");
#endif
            VsyncSystem.RunAsWorker (master, 1800000);
            vsyncGroup = new Vsync.Group (groupname);

            // Start printing the state of the system.
//          Console.Out.WriteLine ("Start printer of Vsync state.");
//          statePrinterThread = new Thread (delegate() {
//			    StatePrinter statePrinter = new StatePrinter (10);
//			    statePrinter.Run ();
//		    }
//		    );
//		    statePrinterThread.Start ();

            // Start the Vsync Group.
#if DEBUG
            WriteLine (workerDebugFilename, "Start as a worker.");
#endif
            VsyncSystem.Start (false);

            // Add handlers to the worker.
#if DEBUG
            WriteLine (workerDebugFilename, "Add handlers.");
#endif
            AddHandlers (vsyncGroup);

            // Now we activate all the Workers simultaneously.
#if DEBUG
            WriteLine (workerDebugFilename, "Setup Done.");
#endif
            VsyncSystem.WorkerSetupDone ();
            return vsyncGroup;
        }

        private Vsync.Group BeMember (string groupname, string memberDebugFilename)
        {
            Vsync.Group vsyncGroup;
            // Thread statePrinterThread;

            // Form the master Vsync Address from a file.
#if DEBUG
            WriteLine (memberDebugFilename, "Run as group member.");
#endif

            // Start printing the state of the system.
//			Console.Out.WriteLine ("Start printer of Vsync state.");
//			statePrinterThread = new Thread (delegate() {
//				StatePrinter statePrinter = new StatePrinter (10);
//				statePrinter.Run ();
//			}
//			);
//			statePrinterThread.Start ();

            // Start the Vsync Group.
#if DEBUG
            WriteLine (memberDebugFilename, "Start as a member.");
#endif
            VsyncSystem.Start ();
            vsyncGroup = new Vsync.Group (groupname);

            // Add handlers to the member.
#if DEBUG
            WriteLine (memberDebugFilename, "Add handlers.");
#endif
            AddHandlers (vsyncGroup);

            // Join the group.
#if DEBUG
            WriteLine (memberDebugFilename, "Joining the Group.");
#endif
            vsyncGroup.Join ();
            return vsyncGroup;
        }
        /*
		public void ReportTimes ()
		{
			Directory.SetCurrentDirectory (_curDirectory);

			using (System.IO.StreamWriter file = new System.IO.StreamWriter(@"cloudmake_local_perf.txt")) {
				for (int i = 0; i < _cloudManagerLocalTimes.Count; i++)
					file.WriteLine (_cloudManagerLocalTimes [i].ToString ());
			}

			if (_isLeader) {
				using (System.IO.StreamWriter file = new System.IO.StreamWriter(@"cloudmake_perf.txt")) {
					for (int i = 0; i < _cloudMakeTimes.Count; i++)
						file.WriteLine (_cloudMakeTimes [i].ToString ());
				}

				using (System.IO.StreamWriter file = new System.IO.StreamWriter(@"cloudmake_leader_perf.txt")) {
					for (int i = 0; i < _cloudManagerLeaderTimes.Count; i++)
						file.WriteLine (_cloudManagerLeaderTimes [i].ToString ());
				}
			}
		}
		*/
        private void WriteLine (string str)
        {
            Console.Out.WriteLine (str);
            using (StreamWriter file = File.AppendText (_debugFilename)) {
                file.WriteLine (str);
            }
        }

        public static void WriteLine (string filename, string str)
        {
            Console.Out.WriteLine (str);
            using (StreamWriter file = File.AppendText (filename)) {
                file.WriteLine (str);
            }
        }

        public static bool IsUnix ()
        {
            int p = (int)Environment.OSVersion.Platform;
            return (p == 4) || (p == 6) || (p == 128);
        }

        public static void Main (string[] args)
        {
            string hostname, groupname, parseTree, master, mainDebugFilename, masterDebugFilename, workerDebugFilename;
            string memberDebugFilename, leaderDebugFilename, localDebugFilename, monitorDebugFilename;
            bool isDeterministic;
            int nrWorkers, nrReplicas, timeout;
            bool isMaster = false;
            bool isWorker = false;
            bool isLeader = false;
            bool isLocal = false;
            Vsync.Group vsyncGroup;

            // Take arguments by parsing the command line.
            try {
                Options options = new Options ();
                Parser parser = new Parser ();

                if (!parser.ParseArguments (args, options)) {
                    Console.Out.WriteLine ("Wrong arguments.");
                    Console.Out.WriteLine (options.GetUsage ());
                    System.Environment.Exit (0);
                }

                hostname = options.hostname;
                groupname = options.groupname;
                parseTree = options.parseTree;
                nrWorkers = options.nrWorkers;
                master = options.master;
                nrReplicas = options.nrReplicas;
                isDeterministic = options.isDeterministic;
                timeout = options.timeout;
                mainDebugFilename = Path.Combine (Directory.GetCurrentDirectory (), options.debugFilename);
                masterDebugFilename = Path.Combine (Directory.GetCurrentDirectory (), options.masterDebugFilename);
                workerDebugFilename = Path.Combine (Directory.GetCurrentDirectory (), options.workerDebugFilename);
                memberDebugFilename = Path.Combine (Directory.GetCurrentDirectory (), options.memberDebugFilename);
                leaderDebugFilename = Path.Combine (Directory.GetCurrentDirectory (), options.leaderDebugFilename);
                localDebugFilename = Path.Combine (Directory.GetCurrentDirectory (), options.localDebugFilename);
                monitorDebugFilename = Path.Combine (Directory.GetCurrentDirectory (), options.monitorDebugFilename);

                if (nrWorkers >= 0)
                    isMaster = true;
                else if (master != null)
                    isWorker = true;
                if (timeout >= 0)
                    isLocal = true;
                else
                    isLeader = true;
                    
                if (isMaster && isWorker) {
                    Console.Out.WriteLine ("This process cannot be a Master and a Worker simultaneously.");
                    Console.Out.WriteLine (options.GetUsage ());
                    System.Environment.Exit (0);
                }

                if (!(isLeader || isLocal)) {
                    Console.Out.WriteLine ("This process should be either a Leader or a Local Daemon or both.");
                    Console.Out.WriteLine (options.GetUsage ());
                    System.Environment.Exit (0);
                }
                
                if (File.Exists (mainDebugFilename))
                    File.Delete (mainDebugFilename);
                using (FileStream fs = File.Create (mainDebugFilename)) {
                }
                ;

#if EC2
				// Set Environmental Variables.
				Environment.SetEnvironmentVariable ("VSYNC_UNICAST_ONLY", "true"); 
				Environment.SetEnvironmentVariable ("VSYNC_SUBNET", "10.0.0.0");
				Environment.SetEnvironmentVariable ("VSYNC_NETMASK", "255.0.0.0");
#endif
                Environment.SetEnvironmentVariable ("VSYNC_DEFAULTTIMEOUT", "30000");

#if DEBUG
                WriteLine (mainDebugFilename, "Group: " + groupname + ", Node: " + hostname);
                if (isMaster || isWorker)
                    WriteLine (mainDebugFilename, "MasterMode: Enabled");
                else
                    WriteLine (mainDebugFilename, "MasterMode: Disabled");
                if (isMaster)
                    WriteLine (mainDebugFilename, "Number of Workers: " + nrWorkers.ToString ());
                if (isWorker)
                    WriteLine (mainDebugFilename, "Master: " + master);
                WriteLine (mainDebugFilename, "Is Leader: " + isLeader.ToString ());
#endif
                /* if (Directory.Exists (hostname))
                    Directory.Delete (hostname, true);
                Directory.CreateDirectory (hostname);*/

                // Initiate CloudManager group.
                CloudManager cloudMake = new CloudManager (hostname, parseTree, isLeader, timeout, nrReplicas,
                                             isDeterministic, mainDebugFilename, localDebugFilename, monitorDebugFilename,
                                             leaderDebugFilename);
                if (isMaster)
                    vsyncGroup = cloudMake.BeMaster (groupname, nrWorkers, masterDebugFilename);
                else if (isWorker)
                    vsyncGroup = cloudMake.BeWorker (groupname, master, workerDebugFilename);
                else
                    vsyncGroup = cloudMake.BeMember (groupname, memberDebugFilename);
#if DEBUG
                WriteLine (mainDebugFilename, "Start Normal Operation.");
#endif
                if (!isMaster && (master != null)) {
                    Thread.Sleep (10000);
                    HashSet<string> waitMembers = new HashSet<string> (cloudMake._waitMembers);
                    foreach (string member in waitMembers) {
                        int start = member.IndexOf (":") + 1;
                        int count = member.IndexOf ("/") - start;
                        string temp = member.Substring (start, count);

                        cloudMake.WriteLine (temp);
                        if (temp != master)
                            cloudMake._waitMembers.Remove (member);
                    }
                }

                Thread mainThread = new Thread (delegate() {
                    cloudMake.Run (vsyncGroup);
                });
                mainThread.Start ();

                // Wait for termination signals.
                if (CloudManager.IsUnix ()) {
                    UnixSignal[] signals = new UnixSignal[] {
                        new UnixSignal (Signum.SIGINT),
                        new UnixSignal (Signum.SIGTERM),
                    };
#if DEBUG
                    WriteLine (mainDebugFilename, "Got a " + signals [UnixSignal.WaitAny (signals, -1)].Signum +
                    " signal!");
#endif
                    VsyncSystem.Shutdown ();
                } else {
                    Console.CancelKeyPress += delegate {
#if DEBUG
                        WriteLine (mainDebugFilename, "Got a CTRL-c signal!");
#endif
                        VsyncSystem.Shutdown ();
                    };
                    Thread.Sleep (System.Threading.Timeout.Infinite);
                }
            } catch (Exception e) {
                Console.Error.WriteLine ("Error " + e.ToString ());
            }
        }
    }
}