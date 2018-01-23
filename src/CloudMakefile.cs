using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Xml;
using System.Xml.Schema;
using System.Runtime.Serialization.Formatters;
using System.Resources;
using System.Security.Policy;

// using Mono.Posix;

namespace CloudManager
{
    public class CloudMakefile
    {
        private List<HashSet<int>> _inputs;
        private List<HashSet<int>> _outputs;
        private List<HashSet<int>> _reverseInputs;
        private List<HashSet<int>> _reverseOutputs;
        private List<DFA> _dfas;
        private List<string> _policies;
        private List<HashSet<int>> _order;

        public CloudMakefile ()
        {
            _inputs = new List<HashSet<int>> ();
            _outputs = new List<HashSet<int>> ();
            _reverseInputs = new List<HashSet<int>> ();
            _reverseOutputs = new List<HashSet<int>> ();
            _dfas = new List<DFA> ();
            _policies = new List<string> ();
            _order = new List<HashSet<int>> ();
        }

        public CloudMakefile (CloudMakefile cloudMakefile)
        {
            List<HashSet<int>> inputs = cloudMakefile.GetInputs ();
            List<HashSet<int>> outputs = cloudMakefile.GetOutputs ();
            List<HashSet<int>> reverseInputs = cloudMakefile.GetReverseInputs ();
            List<HashSet<int>> reverseOutputs = cloudMakefile.GetReverseOutputs ();
            List<HashSet<int>> order = cloudMakefile.GetOrder ();

            _inputs = new List<HashSet<int>> ();
            for (int i = 0; i < inputs.Count; i++)
                _inputs.Add (new HashSet<int> (inputs [i]));
            _outputs = new List<HashSet<int>> ();
            for (int i = 0; i < outputs.Count; i++)
                _outputs.Add (new HashSet<int> (outputs [i]));
            _reverseInputs = new List<HashSet<int>> ();
            for (int i = 0; i < reverseInputs.Count; i++)
                _reverseInputs.Add (new HashSet<int> (reverseInputs [i]));
            _reverseOutputs = new List<HashSet<int>> ();
            for (int i = 0; i < reverseOutputs.Count; i++)
                _reverseOutputs.Add (new HashSet<int> (reverseOutputs [i]));
            _dfas = new List<DFA> (cloudMakefile.GetDFAs ());
            _policies = new List<string> (cloudMakefile.GetPolicies ());
            _order = new List<HashSet<int>> ();
            for (int i = 0; i < order.Count; i++)
                _order.Add (new HashSet<int> (order [i]));
        }

        public CloudMakefile (CloudMakefile cloudMakefile, List<int> selectedPolicies) : this ()
        {
            List<HashSet<int>> inputs = cloudMakefile.GetInputs ();
            List<HashSet<int>> outputs = cloudMakefile.GetOutputs ();
            List<DFA> dfas = cloudMakefile.GetDFAs ();
            List<string> policies = cloudMakefile.GetPolicies ();
            List<HashSet<int>> order = cloudMakefile.GetOrder ();
            Dictionary<int,int> dfaTransform = new Dictionary<int, int> ();
            int dfaCount = 0;
            int newDfaIndex;
            int count = -1;

            for (int newPolicyIndex = 0; newPolicyIndex < selectedPolicies.Count; newPolicyIndex++) {
                int oldPolicyIndex = selectedPolicies [newPolicyIndex];

                _inputs.Add (new HashSet<int> ());
                _outputs.Add (new HashSet<int> ());
                foreach (int oldDfaIndex in inputs[oldPolicyIndex]) {
                    if (!dfaTransform.ContainsKey (oldDfaIndex)) {
                        dfaTransform [oldDfaIndex] = dfaCount;
                        _dfas.Add (dfas [oldDfaIndex]);
                        dfaCount++;
                        _reverseInputs.Add (new HashSet<int> ());
                        _reverseOutputs.Add (new HashSet<int> ());
                    }
                    newDfaIndex = dfaTransform [oldDfaIndex];
                    _inputs [newPolicyIndex].Add (newDfaIndex);
                    _reverseInputs [newDfaIndex].Add (newPolicyIndex);
                }
                foreach (int oldDfaIndex in outputs[oldPolicyIndex]) {
                    if (!dfaTransform.ContainsKey (oldDfaIndex)) {
                        dfaTransform [oldDfaIndex] = dfaCount;
                        _dfas.Add (dfas [oldDfaIndex]);
                        dfaCount++;
                        _reverseInputs.Add (new HashSet<int> ());
                        _reverseOutputs.Add (new HashSet<int> ());
                    }
                    newDfaIndex = dfaTransform [oldDfaIndex];
                    _outputs [newPolicyIndex].Add (newDfaIndex);
                    _reverseOutputs [newDfaIndex].Add (newPolicyIndex);
                }
                _policies.Add (policies [oldPolicyIndex]);
            }

            for (int i = 0; i < order.Count; i++) {
                bool created = false;

                foreach (int oldPolicyIndex in order[i]) {
                    if (selectedPolicies.Contains (oldPolicyIndex)) {
                        if (!created) {
                            _order.Add (new HashSet<int> ());
                            created = true;
                            count += 1;
                        }
                        _order [count].Add (selectedPolicies.IndexOf (oldPolicyIndex));
                    }
                }
            }
        }

        public List<DFA> GetDFAs ()
        {
            return _dfas;
        }

        public List<HashSet<int>> GetInputs ()
        {
            return _inputs;
        }

        public List<HashSet<int>> GetOutputs ()
        {
            return _outputs;
        }

        public List<HashSet<int>> GetReverseInputs ()
        {
            return _reverseInputs;
        }

        public List<HashSet<int>> GetReverseOutputs ()
        {
            return _reverseOutputs;
        }

        public List<string> GetPolicies ()
        {
            return _policies;
        }

        public List<HashSet<int>> GetOrder ()
        {
            return _order;
        }

        public override string ToString ()
        {
            string res = "CloudMakefile:" + Environment.NewLine + Environment.NewLine;

            for (int curRule = 0; curRule < _inputs.Count; curRule++) {
                res += "Rule " + curRule.ToString () + Environment.NewLine;
                res += "Inputs:" + Environment.NewLine;
                foreach (int i in _inputs[curRule])
                    res += _dfas [i].ToString ();
                res += "Outputs:" + Environment.NewLine;
                foreach (int i in _outputs[curRule])
                    res += _dfas [i].ToString ();
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

            return res;
        }

        public void ParseMakefileTree (string parseTreeFilename)
        {
            string[] lines = File.ReadAllLines (@parseTreeFilename);
            int currentRule = 0;

            foreach (string line in lines)
                currentRule = ParseFRule (new Node (line), currentRule, new Dictionary<string, List<Node>> ());
        }

        private int ParseFRule (Node fruleNode, int currentRule, Dictionary<string,List<Node>> vars)
        {
            Node.Type type = fruleNode.GetNodeType ();
            List<Node> children, grandchildren;
            string varName;

            switch (type) {
            case(Node.Type.TSRULE):
                children = fruleNode.GetChildren ();
                varName = ParseString (children [0]);
                if (vars.ContainsKey (varName))
                    throw new CloudMakefile.ParseException ("There should not be two variables with the same name " +
                    "within the same rule.");
                grandchildren = children [1].GetChildren ();
                for (int i = 0; i < grandchildren.Count; i++)
                    if (grandchildren [i].GetNodeType () != Node.Type.TSEQUENCE)
                        throw new CloudMakefile.ParseException ("TSRULE type should only have TSEQUENCE types after " +
                        "TVAR type.");
                vars.Add (varName, grandchildren);
                return ParseFRule (children [2], currentRule, vars);
            case(Node.Type.TNRULE):
                children = fruleNode.GetChildren ();
                varName = ParseString (children [0]);
                if (vars.ContainsKey (varName))
                    throw new CloudMakefile.ParseException ("There should not be two variables with the same name " +
                    "within the same rule.");
                grandchildren = children [1].GetChildren ();
                if ((grandchildren.Count != 2) || (grandchildren [0].GetNodeType () != Node.Type.TSEQUENCE) ||
                    (grandchildren [1].GetNodeType () != Node.Type.TSEQUENCE))
                    throw new CloudMakefile.ParseException ("TNRULE should have two children under TSEQUENCE TYPE.");

                int start = ParseInt (grandchildren [0]);
                int end = ParseInt (grandchildren [1]);

                vars.Add (varName, new List<Node> ());
                for (int i = start; i <= end; i++)
                    vars [varName].Add (MakeNodeFromInt (i));
                return ParseFRule (children [2], currentRule, vars);
            case(Node.Type.TRULE):
                return ParseMultipleRule (fruleNode, currentRule, vars);
            default:
                throw new CloudMakefile.ParseException ("The rule must be of type TSRULE, TNRULE or TRULE.");
            }
        }

        private DFA SubstituteEntries (List<Entry> entries, Dictionary<string, NFA> vars)
        {
            NFA nfa = new NFA ();

            foreach (Entry entry in entries) {
                if (entry.IsVar ()) {
                    if (!vars.ContainsKey (entry.GetVar ()))
                        throw new CloudMakefile.ParseException ("Variable " + entry.GetVar () + " is not defined.");
                    nfa.SequentialComposition (vars [entry.GetVar ()]);
                } else {
                    nfa.SequentialComposition (entry.GetNFA ());
                }
            }

            return nfa.ConvertToDFA ();
        }

        private void AddDFA (DFA dfa, bool isInput, int currentRule)
        {
            int count = _dfas.Count;

            for (int i = 0; i < count; i++) {
                DFA curDfa = _dfas [i];
                DFA intersectDfa = dfa * curDfa;

                if (intersectDfa != null) {
                    _dfas [i] = intersectDfa;
                    if (isInput) {
                        _inputs [currentRule].Add (i);
                        _reverseInputs [i].Add (currentRule);
                    } else {
                        _outputs [currentRule].Add (i);
                        _reverseOutputs [i].Add (currentRule);
                    }

                    DFA subtractDfa = curDfa - intersectDfa;

                    if (subtractDfa != null) {
                        int nextDfa = _dfas.Count;

                        _dfas.Add (subtractDfa);
                        _reverseInputs.Add (new HashSet<int> (_reverseInputs [i]));
                        _reverseOutputs.Add (new HashSet<int> (_reverseOutputs [i]));
                        if (isInput)
                            _reverseInputs [nextDfa].Remove (currentRule);
                        else
                            _reverseOutputs [nextDfa].Remove (currentRule);
                        foreach (int rule in _reverseInputs[nextDfa])
                            _inputs [rule].Add (nextDfa);
                        foreach (int rule in _reverseOutputs[nextDfa])
                            _outputs [rule].Add (nextDfa);
                    }

                    dfa = dfa - intersectDfa;

                    if (dfa == null)
                        break;
                }
            }

            if (dfa != null) {
                int nextDfa = _dfas.Count;

                _dfas.Add (dfa);
                _reverseInputs.Add (new HashSet<int> ());
                _reverseOutputs.Add (new HashSet<int> ());
                if (isInput) {
                    _inputs [currentRule].Add (nextDfa);
                    _reverseInputs [nextDfa].Add (currentRule);
                } else {
                    _outputs [currentRule].Add (nextDfa);
                    _reverseOutputs [nextDfa].Add (currentRule);
                }
            }
        }

        private void ParseSingleRule (List<List<Entry>> inputEntries, List<List<Entry>> outputEntries,
                                      string policy, int currentRule, Dictionary<string, NFA> vars)
        {
            _inputs.Add (new HashSet<int> ());
            _outputs.Add (new HashSet<int> ());

            foreach (List<Entry> entries in inputEntries)
                AddDFA (SubstituteEntries (entries, vars), true, currentRule);
            foreach (List<Entry> entries in outputEntries)
                AddDFA (SubstituteEntries (entries, vars), false, currentRule);

            _policies.Add (policy);
        }

        private int ParseMultipleRule (List<List<Entry>> inputEntries, List<List<Entry>> outputEntries,
                                       string policy, int currentRule, Dictionary<string, List<Node>> mVars,
                                       Dictionary<string,NFA> vars)
        {
            if (mVars.Count == 0) {
                ParseSingleRule (inputEntries, outputEntries, policy, currentRule, vars);
                return currentRule + 1;
            }

            string varName = mVars.First ().Key;
            List<Node> nodeList = mVars [varName];
            int nextRule = currentRule;

            foreach (Node node in nodeList) {
                Dictionary<string, List<Node>> tempMVars = new Dictionary<string, List<Node>> (mVars);
                Dictionary<string,NFA> tempVars = new Dictionary<string, NFA> (vars);
                List<Entry> entryList = NFA.ParseTreeNode (node);

                if ((entryList.Count != 1) || (entryList [0].IsVar () != false))
                    throw new CloudMakefile.ParseException ("Variables should return only one NFA when parsed.");
                tempMVars.Remove (varName);
                tempVars [varName] = entryList [0].GetNFA ();
                nextRule = ParseMultipleRule (inputEntries, outputEntries, policy, nextRule, tempMVars, tempVars);
            }
            return nextRule;
        }

        private int ParseMultipleRule (Node ruleNode, int currentRule, Dictionary<string,List<Node>> vars)
        {
            List<Node> children = ruleNode.GetChildren ();
            List<List<Entry>> inputEntries = new List<List<Entry>> ();
            List<List<Entry>> outputEntries = new List<List<Entry>> ();
            String policy;

            foreach (Node node in children[0].GetChildren()) {
                if ((node.GetNodeType () != Node.Type.TENTRY) && (node.GetNodeType () != Node.Type.TEVENT))
                    throw new CloudMakefile.ParseException ("The children of TENTRIES node should be of TENTRY type.");
                outputEntries.Add (NFA.ParseTreeNode (node));
            }
            foreach (Node node in children[1].GetChildren()) {
                if ((node.GetNodeType () != Node.Type.TENTRY) && (node.GetNodeType () != Node.Type.TEVENT))
                    throw new CloudMakefile.ParseException ("The children of TENTRIES node should be of TENTRY type.");
                inputEntries.Add (NFA.ParseTreeNode (node));
            }
            policy = ParseString (children [2]);

            return ParseMultipleRule (inputEntries, outputEntries, policy, currentRule, vars,
                new Dictionary<string, NFA> ());
        }

        static public int ParseInt (Node node)
        {
            return Int32.Parse (ParseString (node));
        }

        static public string ParseString (Node node)
        {
            List<Node> children = node.GetChildren ();
            string res = "";

            foreach (Node charNode in children) {
                if (charNode.GetNodeType () != Node.Type.TCHAR)
                    throw new CloudMakefile.ParseException ("The string must consist of TCHAR nodes.");
                res += charNode.GetChar ().ToString ();
            }
            return res;
        }

        static public Node MakeNodeFromInt (int n)
        {
            return MakeNodeFromString (n.ToString ());
        }

        static public Node MakeNodeFromString (string str)
        {
            string nodeStr = "SEQUENCE(";

            foreach (char c in str.ToCharArray())
                nodeStr += "CHAR(" + c.ToString () + "),";
            nodeStr = nodeStr.Substring (0, nodeStr.Length - 1);
            nodeStr += ")";

            return new Node (nodeStr);
        }

        public class ParseException : Exception
        {
            public ParseException ()
            {
            }

            public ParseException (string message)
                : base (message)
            {
            }

            public ParseException (string message, Exception inner)
                : base (message, inner)
            {
            }
        }

        public bool DetectCycles ()
        {
            HashSet<int> availableTransitions = new HashSet<int> ();
            int oldCount;

            for (int i = 0; i < _inputs.Count; i++)
                availableTransitions.Add (i);
            oldCount = availableTransitions.Count + 1;
            while ((availableTransitions.Count > 0) && (availableTransitions.Count < oldCount)) {
                HashSet<int> aggOutputs = new HashSet<int> ();
                HashSet<int> firstOrder = new HashSet<int> ();

                oldCount = availableTransitions.Count;
                foreach (int tr in availableTransitions) {
                    foreach (int output in _outputs[tr])
                        aggOutputs.Add (output);
                }
                foreach (int tr in availableTransitions) {
                    bool flag = true;

                    foreach (int input in _inputs[tr]) {
                        if (aggOutputs.Contains (input)) {
                            flag = false;
                            break;
                        }
                    }
                    if (flag)
                        firstOrder.Add (tr);
                }
                _order.Add (firstOrder);
                foreach (int tr in firstOrder)
                    availableTransitions.Remove (tr);
            }

            if (availableTransitions.Count > 0)
                return true;

            return false;
        }

        public List<Tuple<string,int>> FindMatchingEntries (string entry)
        {
            HashSet<int> dfas = new HashSet<int> ();

            for (int i = 0; i < _dfas.Count; i++)
                dfas.Add (i);

            return FindMatchingEntries (entry, dfas);
        }

        public List<Tuple<string,int>> FindMatchingEntries (string entry, HashSet<int> dfas)
        {
            List<Tuple<string,int>> res = new List<Tuple<string, int>> ();
            List<Tuple<string, HashSet<int>>> unexplored = new List<Tuple<string, HashSet<int>>> ();

            unexplored.Add (new Tuple<string, HashSet<int>> (entry, dfas));
            while (unexplored.Count != 0) {
                string curPath = unexplored [0].Item1;
                HashSet<int> curDfaList = unexplored [0].Item2;
                HashSet<int> tempDfaList = new HashSet<int> ();
                HashSet<string> children = new HashSet<string> ();

                if (curPath.EndsWith (".xml")) {
                    XmlDocument doc = new XmlDocument ();

                    doc.Load (curPath);
                    children.Add (curPath + "{" + doc.DocumentElement.Name + "}");
                } else if (curPath.Contains (".xml")) {
                    int index = curPath.IndexOf (".xml") + 4;
                    string filepath = curPath.Substring (0, index);
                    string xmlpath = curPath.Substring (index);
                    string[] xmlFields = xmlpath.Split ('{', '}');
                    XmlDocument doc = new XmlDocument ();
                    string xpath = xmlFields [0];
                    XmlNodeList xmlNodeList;

                    for (int i = 1; i < xmlFields.Length; i++)
                        xpath += '/' + xmlFields [i];
                    doc.Load (filepath);
                    xmlNodeList = doc.SelectNodes (xpath);

                    foreach (XmlNode node in xmlNodeList) {
                        if (node.HasChildNodes)
                            foreach (XmlNode childNode in node.ChildNodes)
                                children.Add (curPath + "{" + childNode.Name + "}");
                    }
                } else if (Directory.Exists (curPath) || curPath == "") {
                    string path = (curPath == "") ? Directory.GetCurrentDirectory () : curPath;
                    DirectoryInfo curDir = new DirectoryInfo (path);
                    FileInfo[] files = curDir.GetFiles ("*", SearchOption.TopDirectoryOnly);
                    DirectoryInfo[] dirs = curDir.GetDirectories ("*", SearchOption.TopDirectoryOnly);

                    path = (curPath == "") ? curPath : curPath + '/';
                    foreach (FileInfo file in files)
                        children.Add (path + file.Name);
                    foreach (DirectoryInfo dir in dirs)
                        children.Add (path + dir.Name);
                }

                foreach (int i in dfas) {
                    FAResponse response = _dfas [i].Parse (curPath);

                    switch (response) {
                    case FAResponse.ACCEPT:
                        res.Add (new Tuple<string, int> (curPath, i));
                        break;
                    case FAResponse.NOT_REJECT:
                        tempDfaList.Add (i);
                        break;
                    case FAResponse.REJECT:
                        break;
                    }
                }

                if (tempDfaList.Count != 0)
                    foreach (string child in children)
                        unexplored.Add (new Tuple<string, HashSet<int>> (child, tempDfaList));

                unexplored.RemoveAt (0);
            }

            return res;
        }

        public HashSet<int> Compatible (string str)
        {
            HashSet<int> dfas = new HashSet<int> ();

            for (int i = 0; i < _dfas.Count; i++)
                dfas.Add (i);

            return Compatible (str, dfas);
        }

        public HashSet<int> Compatible (string str, HashSet<int> dfas)
        {
            HashSet<int> res = new HashSet<int> ();

            foreach (int i in dfas) {
                FAResponse response = _dfas [i].Parse (str);

                if ((response == FAResponse.ACCEPT) || (response == FAResponse.NOT_REJECT))
                    res.Add (i);
            }

            return res;
        }

        static public List<CloudMakefile> SplitCloudMakefile (CloudMakefile initialCloudMakefile)
        {
            List<CloudMakefile> res = new List<CloudMakefile> ();
            List<List<int>> selectedPolicies = new List<List<int>> ();
            List<int> unexploredPolicies = new List<int> ();
            List<int> unexploredDfas = new List<int> ();
            List<int> unsearchedPolicies = new List<int> ();
            List<int> unsearchedDfas = new List<int> ();
            List<HashSet<int>> inputs = initialCloudMakefile.GetInputs ();
            List<HashSet<int>> outputs = initialCloudMakefile.GetOutputs ();
            List<HashSet<int>> reverseInputs = initialCloudMakefile.GetReverseInputs ();
            List<HashSet<int>> reverseOutputs = initialCloudMakefile.GetReverseOutputs ();
            int count = 0;
            bool explorePolicy;

            // Fill the unsearched fields.
            for (int i = 0; i < initialCloudMakefile.GetPolicies ().Count; i++)
                unsearchedPolicies.Add (i);
            for (int i = 0; i < initialCloudMakefile.GetDFAs ().Count; i++)
                unsearchedDfas.Add (i);

            // Divide CloudMakefile to independent pieces.
            while (unsearchedPolicies.Count != 0) {
//				Console.Out.WriteLine ("Unsearched Policies");
//				for (int i = 0; i < unsearchedPolicies.Count; i++)
//					Console.Out.WriteLine (unsearchedPolicies [i].ToString ());
//				Console.Out.WriteLine ("Unsearched DFAs");
//				for (int i = 0; i < unsearchedDfas.Count; i++)
//					Console.Out.WriteLine (unsearchedDfas [i].ToString ());
                unexploredPolicies.Add (unsearchedPolicies [0]);
                unsearchedPolicies.RemoveAt (0);
                explorePolicy = true;
                selectedPolicies.Add (new List<int> ());
                while ((unexploredPolicies.Count != 0) || (unexploredDfas.Count != 0)) {
                    if (explorePolicy) {
                        while (unexploredPolicies.Count != 0) {
                            int policyIndex = unexploredPolicies [0];

                            /*
							Console.Out.WriteLine ("Selected Policy: " + policyIndex);
							Console.Out.WriteLine ("Inputs:" + String.Join (" ", inputs [policyIndex]));
							Console.Out.WriteLine ("Outputs:" + String.Join (" ", outputs [policyIndex]));
							*/
                            foreach (int dfaIndex in inputs[policyIndex]) {
                                if (unsearchedDfas.Contains (dfaIndex)) {
                                    unexploredDfas.Add (dfaIndex);
                                    unsearchedDfas.Remove (dfaIndex);
                                }
                            }
                            foreach (int dfaIndex in outputs[policyIndex]) {
                                if (unsearchedDfas.Contains (dfaIndex)) {
                                    unexploredDfas.Add (dfaIndex);
                                    unsearchedDfas.Remove (dfaIndex);
                                }
                            }
                            unexploredPolicies.RemoveAt (0);
                            selectedPolicies [count].Add (policyIndex);
                        }
                    } else {
                        while (unexploredDfas.Count != 0) {
                            int dfaIndex = unexploredDfas [0];

                            /*
							Console.Out.WriteLine ("Selected Index: " + dfaIndex);
							Console.Out.WriteLine ("Reverse Inputs:" + String.Join (" ", reverseInputs [dfaIndex]));
							Console.Out.WriteLine ("Reverse Outputs:" + String.Join (" ", reverseOutputs [dfaIndex]));
							*/
                            foreach (int policyIndex in reverseInputs[dfaIndex]) {
                                if (unsearchedPolicies.Contains (policyIndex)) {
                                    unexploredPolicies.Add (policyIndex);
                                    unsearchedPolicies.Remove (policyIndex);
                                }
                            }
                            foreach (int policyIndex in reverseOutputs[dfaIndex]) {
                                if (unsearchedPolicies.Contains (policyIndex)) {
                                    unexploredPolicies.Add (policyIndex);
                                    unsearchedPolicies.Remove (policyIndex);
                                }
                            }
                            unexploredDfas.RemoveAt (0);
                        }
                    }
                    explorePolicy = !explorePolicy;
                }
                count++;
            }

            // Print Selected Policies.
            /*for (int i = 0; i < selectedPolicies.Count; i++) {
				Console.Out.Write ("[ ");
				selectedPolicies [i].Sort ();
				for (int j = 0; j < selectedPolicies[i].Count; j++)
					Console.Out.Write (selectedPolicies [i] [j].ToString () + " ");
				Console.Out.WriteLine ("]");
			}*/

            // Create the CloudMakefiles.
            for (int i = 0; i < selectedPolicies.Count; i++)
                res.Add (new CloudMakefile (initialCloudMakefile, selectedPolicies [i]));

            return res;
        }

        public bool IsLocalCloudMakefile ()
        {
            string nodename = GetNode ();

            if (nodename == null)
                return false;
            foreach (DFA dfa in _dfas.GetRange(1,_dfas.Count-1))
                if (!dfa.ParseLocal (nodename))
                    return false;
            return true;
        }

        public bool IsLocalCloudMakefile (string nodename)
        {
            foreach (DFA dfa in _dfas)
                if (!dfa.ParseLocal (nodename))
                    return false;
            return true;
        }

        public string GetNode ()
        {
            return _dfas [0].GetNode ();
        }
    }
}