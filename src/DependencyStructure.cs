using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Xml;
using System.Xml.Schema;

namespace CloudManager
{
    public class DependencyStructure
    {
        private CloudMakefile _cloudMakefile;
        private List<string> _entries;
        private List<string> _policies;
        private List<List<int>> _inputs;
        private List<List<int>> _outputs;
        private List<List<int>> _reverseInputs;
        private List<List<int>> _reverseOutputs;
        private HashSet<int> _entryTokens;
        private HashSet<int> _policyTokens;
        private List<HashSet<int>> _order;
        private HashSet<string> _configFiles;
        private HashSet<string> _modifiedConfigFiles;
        private HashSet<string> _cloudMakeConfigFiles;
        private HashSet<string> _modifiedCloudMakeConfigFiles;
        private string _debugFilename;

        public DependencyStructure (CloudMakefile cloudMakefile, string debugFilename)
        {
            CloudManager.WriteLine (debugFilename, "CloudMakefile: " + cloudMakefile.ToString ());
            _cloudMakefile = cloudMakefile;
            _entries = new List<string> ();
            _policies = cloudMakefile.GetPolicies ();
            _inputs = new List<List<int>> ();
            _outputs = new List<List<int>> ();
            for (int i = 0; i < _policies.Count; i++) {
                _inputs.Add (new List<int> ());
                _outputs.Add (new List<int> ());
            }
            _reverseInputs = new List<List<int>> ();
            _reverseOutputs = new List<List<int>> ();
            _entryTokens = new HashSet<int> ();
            _policyTokens = new HashSet<int> ();
            _order = cloudMakefile.GetOrder ();
            _configFiles = new HashSet<string> ();
            _modifiedConfigFiles = new HashSet<string> ();
            _cloudMakeConfigFiles = new HashSet<string> ();
            _modifiedCloudMakeConfigFiles = new HashSet<string> ();
            _debugFilename = debugFilename;
        }

        public List<string> GetEntries ()
        {
            return _entries;
        }

        public HashSet<string> GetConfigFiles ()
        {
            return _configFiles;
        }

        public HashSet<string> GetModifiedConfigFiles ()
        {
            return _modifiedConfigFiles;
        }

        public HashSet<string> GetCloudMakeConfigFiles ()
        {
            return _cloudMakeConfigFiles;
        }

        public HashSet<string> GetModifiedCloudMakeConfigFiles ()
        {
            return _modifiedCloudMakeConfigFiles;
        }

        public override string ToString ()
        {
            string res = "DEPENDENCY_STRUCTURE:" + Environment.NewLine + Environment.NewLine;

            for (int curRule = 0; curRule < _inputs.Count; curRule++) {
                res += "Rule " + curRule.ToString () + Environment.NewLine;
                res += "Inputs:" + Environment.NewLine;
                foreach (int i in _inputs[curRule])
                    res += _entries [i].ToString () + Environment.NewLine;
                res += "Outputs:" + Environment.NewLine;
                foreach (int i in _outputs[curRule])
                    res += _entries [i].ToString () + Environment.NewLine;
                res += "Action: " + _policies [curRule] + Environment.NewLine + Environment.NewLine;
            }

            res += "Order:" + Environment.NewLine;
            for (int i = 0; i < _order.Count; i++) {
                res += i.ToString () + ":";
                foreach (int tr in _order[i])
                    res += " " + tr.ToString ();
                res += Environment.NewLine;
            }
            res += Environment.NewLine;

            res += "Entry Tokens:" + Environment.NewLine;
            foreach (int token in _entryTokens) {
                res += _entries [token] + Environment.NewLine;
            }
            res += Environment.NewLine;

            res += "Policy Tokens:" + Environment.NewLine;
            foreach (int token in _policyTokens) {
                res += _policies [token] + Environment.NewLine;
            }
            res += Environment.NewLine;

            res += "Config Files:" + Environment.NewLine;
            foreach (string filename in _configFiles) {
                res += filename + Environment.NewLine;
            }
            res += Environment.NewLine;

            res += "Modified Config Files:" + Environment.NewLine;
            foreach (string filename in _modifiedConfigFiles) {
                res += filename + Environment.NewLine;
            }
            res += Environment.NewLine;

            res += "CloudMake Config Files:" + Environment.NewLine;
            foreach (string filename in _cloudMakeConfigFiles) {
                res += filename + Environment.NewLine;
            }
            res += Environment.NewLine;

            res += "Modified CloudMake Config Files:" + Environment.NewLine;
            foreach (string filename in _modifiedCloudMakeConfigFiles) {
                res += filename + Environment.NewLine;
            }
            res += Environment.NewLine;

            return res;
        }
        // Determine if there is anything changed.
        public bool HasActiveEntryTokens ()
        {
            return _entryTokens.Count > 0;
        }
        // Adds entries to dependency structure.
        public void AddEntries (List<Tuple<string,int>> entries)
        {
            foreach (Tuple<string,int> entry in entries)
                AddEntry (entry);
        }
        // Adds entry to dependency structure.
        public void AddEntry (Tuple<string,int> entry)
        {
            string entryname = entry.Item1;
            int id = entry.Item2;
            HashSet<int> reverseInputs = _cloudMakefile.GetReverseInputs () [id];
            HashSet<int> reverseOutputs = _cloudMakefile.GetReverseOutputs () [id];
            int entryIndex = _entries.Count;

            if (_entries.Contains (entryname))
                return;
            _entries.Add (entryname);
            _reverseInputs.Add (new List<int> (reverseInputs));
            _reverseOutputs.Add (new List<int> (reverseOutputs));
            foreach (int policyIndex in reverseInputs)
                _inputs [policyIndex].Add (entryIndex);
            foreach (int policyIndex in reverseOutputs)
                _outputs [policyIndex].Add (entryIndex);
        }
        // Adds token to the specified entry.
        public void AddTokenToEntry (string entryname)
        {
            int entryIndex = _entries.IndexOf (entryname);

            _entryTokens.Add (entryIndex);
        }
        // Adds a new configuration file.
        public void AddConfigFile (string configFilename)
        {
            _configFiles.Add (configFilename);
        }
        // Adds a new CloudMake configuration file.
        public void AddCloudMakeConfigFile (string configFilename)
        {
            _cloudMakeConfigFiles.Add (configFilename);
        }
        // Helper function.
        private string XmlNodeListToString (XmlNodeList nodeList)
        {
            string returnStr = "";

            if (nodeList != null) {
                foreach (XmlNode node in nodeList) {
                    returnStr += node.OuterXml;
                }

            }
            return "<Root>" + returnStr + "</Root>";
        }
        // Helper function.
        private string HashEntry (string entryname)
        {
            if (entryname.EndsWith (".xml")) {
                return CloudManager.GetHash (CloudManager.GetXMLAsString (entryname));
            } else {
                int index = entryname.IndexOf (".xml") + 4;
                string filepath = entryname.Substring (0, index);
                string xmlpath = entryname.Substring (index);
                string[] xmlFields = xmlpath.Split ('{', '}');
                XmlDocument doc = new XmlDocument ();
                string xpath = xmlFields [0];
                XmlNodeList xmlNodeList;

                for (int k = 1; k < xmlFields.Length; k++)
                    xpath += '/' + xmlFields [k];
                doc.Load (filepath);
                xmlNodeList = doc.SelectNodes (xpath);
                return CloudManager.GetHash (XmlNodeListToString (xmlNodeList));
            }
        }

        private string MatchEntryToConfigFile (string entryname)
        {
            foreach (string configFilename in _configFiles)
                if (entryname.StartsWith (configFilename))
                    return configFilename;
            return null;
        }

        private string MatchEntryToCloudMakeConfigFile (string entryname)
        {
            foreach (string cloudMakeConfigFilename in _cloudMakeConfigFiles)
                if (entryname.StartsWith (cloudMakeConfigFilename))
                    return cloudMakeConfigFilename;
            return null;
        }
        // Run CloudMake
        public void Run ()
        {
#if DEBUG
            CloudManager.WriteLine (_debugFilename, "Started Running CloudMake.");
#endif
            List<HashSet<int>> outputs = _cloudMakefile.GetOutputs ();
            HashSet<int> activeEntryTokens = new HashSet<int> (_entryTokens);
            int tier = 0;

            while ((activeEntryTokens.Count > 0) || (_policyTokens.Count > 0)) {
                HashSet<int> entryTokensToRemove = new HashSet<int> (activeEntryTokens);
#if DEBUG
                foreach (int token in activeEntryTokens) {
                    CloudManager.WriteLine (_debugFilename, "Active Entry: " + _entries [token]);
                    foreach (int policyToken in _reverseInputs[token])
                        _policyTokens.Add (policyToken);
                }
#endif
                // If there are no more transitions to run.
                if (tier >= _order.Count)
                    break;

                // Find active policies in this tier of policies.
                HashSet<int> activeTransitions = new HashSet<int> (_order [tier]);
#if DEBUG
                CloudManager.WriteLine (_debugFilename, "Active Transitions:");
                foreach (int transition in activeTransitions) {
                    CloudManager.WriteLine (_debugFilename, "Transition " + transition.ToString ());
                }
                CloudManager.WriteLine (_debugFilename, "");
#endif
                activeTransitions.IntersectWith (_policyTokens);

                // Execute the active policies.
                foreach (int token in activeTransitions) {
                    CloudManager.WriteLine (_debugFilename, "Active Policy: " + _policies [token]);
                    Dictionary<string, string> outputEntries = new Dictionary<string, string> ();
                    string policy = _policies [token];

                    if (policy.Contains ("$(inputs)")) {
                        String inputs = "";
                        HashSet<int> entriesToConsider = new HashSet<int> (_inputs [token]);

                        entriesToConsider.IntersectWith (_entryTokens);
                        foreach (int entry in entriesToConsider) {
                            inputs += " " + _entries [entry];
                        }
                        policy = policy.Replace (" $(inputs)", inputs);
                    }

                    foreach (int entryToken in _outputs[token]) {
                        string entryname = _entries [entryToken];

                        outputEntries.Add (entryname, HashEntry (entryname));
                    }

                    string[] program = policy.Split (new char[] { ' ', '\t' }, 2);

                    CloudManager.WriteLine (_debugFilename, "Directory: " + Directory.GetCurrentDirectory ());
                    CloudManager.WriteLine (_debugFilename, "Execute: " + program [0] + " " + program [1]);

                    using (Process proc = new System.Diagnostics.Process ()) {
                        proc.EnableRaisingEvents = false;
                        proc.StartInfo.FileName = program [0];
                        proc.StartInfo.Arguments = program [1];
                        proc.Start ();
                        proc.WaitForExit ();
                    }
                    ;


                    List<Tuple<string,int>> outputEntriesToConsider =
                        _cloudMakefile.FindMatchingEntries ("", new HashSet<int> (outputs [token]));

                    foreach (Tuple<string,int> entry in outputEntriesToConsider) {
                        string entryname = entry.Item1;

                        if ((outputEntries.ContainsKey (entryname)) &&
                            (HashEntry (entryname) != outputEntries [entryname])) {
                            int entryIndex = _entries.IndexOf (entryname);

                            _entryTokens.Add (entryIndex);
                            activeEntryTokens.Add (entryIndex);
                        } else if (!outputEntries.ContainsKey (entryname)) {
                            _entryTokens.Add (_entries.Count);
                            activeEntryTokens.Add (_entries.Count);
                            AddEntry (entry);
                        }
                    }
                    _policyTokens.Remove (token);
                }

                // Remove the appropriate entries.
                foreach (int token in entryTokensToRemove)
                    activeEntryTokens.Remove (token);
                tier++;
            }

            // Update modifies config and CloudMake config files.
            foreach (int token in _entryTokens) {
                string entryname = _entries [token];
                string filename;

                filename = MatchEntryToConfigFile (entryname);
                if (filename != null)
                    _modifiedConfigFiles.Add (filename);

                filename = MatchEntryToCloudMakeConfigFile (entryname);
                if (filename != null)
                    _modifiedCloudMakeConfigFiles.Add (filename);
            }
        }

        public void Reset ()
        {
            if (_policyTokens.Count != 0) {
#if DEBUG
                CloudManager.WriteLine (_debugFilename, "Policy Tokens:");
                foreach (int policy in _policyTokens)
                    CloudManager.WriteLine (_debugFilename, "Policy Token " + policy.ToString ());
#endif
                throw new Exception ("There should not be tokens in policies.");
            }
            _entryTokens.Clear ();
            _modifiedConfigFiles.Clear ();
            _modifiedCloudMakeConfigFiles.Clear ();
        }
    }
}

