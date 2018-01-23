using CommandLine;
using CommandLine.Text;
using System;

namespace CloudManager
{
    /**
	 * Class for defining all the options that can be passed as parameters in the command line.
	 */
    public class Options
    {
        [Option ('h', "hostname", Required = true, HelpText = "Hostname of the node.")]
        public string hostname { get; set; }

        [Option ('g', "groupname", DefaultValue = "CloudManagerGroup",
            HelpText = "Name of the group formed. Useful if there are two or more CloudManager Groups.")]
        public string groupname { get; set; }

        [Option ('p', "parseTree", Required = true, HelpText = "Parse tree for CloudMakefile")]
        public string parseTree { get; set; }

        [Option ("debugFilename", DefaultValue = "main.debug",
            HelpText = "Debug filename for initiation of CloudManager.")]
        public string debugFilename { get; set; }

        [Option ('w', "nrWorkers", DefaultValue = -1,
            HelpText = "The number defines the number of workers in the group. Only applicable to instances which " +
            "act as masters (see documentation).")]
        public int nrWorkers { get; set; }

        [Option ("masterDebugFilename", DefaultValue = "master.debug",
            HelpText = "Master debug filename for initiation of masters. Only applicable to instances which act as " +
            "masters (see documentation).")]
        public string masterDebugFilename { get; set; }

        [Option ('m', "master",
            HelpText = "The Master IP address. Only applicable to instances which act as workers (see documentation).")]
        public string master { get; set; }

        [Option ("workerDebugFilename", DefaultValue = "worker.debug",
            HelpText = "Worker debug filename for initiation of workers. Only applicable to instances which act as " +
            "workers (see documentation).")]
        public string workerDebugFilename { get; set; }

        [Option ("memberDebugFilename", DefaultValue = "member.debug",
            HelpText = "Member debug filename for initiation of members. Only applicable to instances which act as " +
            "members (see documentation).")]
        public string memberDebugFilename { get; set; }

        [Option ('r', "nrReplicas", DefaultValue = 0,
            HelpText = "Number of replicas needed for management. Only applicable to instances which act as leaders.")]
        public int nrReplicas { get; set; }

        [Option ('d', "deterministic", DefaultValue = true,
            HelpText = "If the management policies are deterministic or not. Only applicable to instances which act " +
            "as leaders.")]
        public bool isDeterministic { get; set; }

        [Option ("leaderDebugFilename", DefaultValue = "leader.debug",
            HelpText = "Leader debug filename. Only applicable to instances which act as leaders (see documentation).")]
        public string leaderDebugFilename { get; set; }

        [Option ('t', "timeout", DefaultValue = -1,
            HelpText = "Time in milliseconds to trigger monitoring of processes. Only applicable to instances which " +
            "act as local daemons (see documentation).")]
        public int timeout { get; set; }

        [Option ("monitorDebugFilename", DefaultValue = "monitor.debug",
            HelpText = "Monitor debug filename. Only applicable to instances which act as local daemons (see " +
            "documentation).")]
        public string monitorDebugFilename { get; set; }

        [Option ("localDebugFilename", DefaultValue = "local.debug",
            HelpText = "Local debug filename. Only applicable to instances which act as local daemons (see " +
            "documentation).")]
        public string localDebugFilename { get; set; }

        [ParserState]
        public IParserState lastParserState { get; set; }

        [HelpOption]
        public string GetUsage ()
        {
            HelpText help = new HelpText ();
            help.Copyright = new CopyrightInfo ("Theodoros Gkountouvas", 2015);
            help.AddPreOptionsLine ("Contact me at my email: tedgoud@cs.cornell.edu");
            help.AddPreOptionsLine ("CloudManager is built on top of Virtual Synchrony (Vsync) library and provides " +
            "a nice abstraction to the user who overcomes all the difficulties that have to do with monitoring and " +
            "configuration of the system. The application designer needs to handle the following tasks:"
            );
            help.AddPreOptionsLine ("1. Launch CloudManager processes in every computer node with the corresponding " +
            "arguments.");
            help.AddPreOptionsLine ("2. Provide information to CloudManager process for every Application Process " +
            "the user needs to run. More specifically an associated id (needs to be unique for the corresponding " +
            "Application Process in the computer node), the executable command, status and config file names (they " +
            "are going to be used for monitoring and configuration).");
            help.AddPreOptionsLine ("3. Provide the necessary monitoring information in the status file specified " +
            "in 2.");
            help.AddPreOptionsLine ("4. Modify the configuration of every Application Process according to the " +
            "config file specified in 2 when necessary.");
            help.AddPreOptionsLine ("5. Provide management policies by writing a CloudMakefile that take as an input " +
            "all status files along with information about new and failed computer nodes and failed application " +
            "processes in the system and produces the appropriate configuration files.");
            help.AddOptions (this);
            return help;
        }
    }
}

