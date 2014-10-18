package edu.rice.habanero.benchmarks.chameneos;

import edu.rice.habanero.benchmarks.BenchmarkRunner;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class ChameneosConfig {

    protected static int numChameneos = 100;
    protected static int numMeetings = 200_000;
    protected static int numMailboxes = 1;
    protected static boolean usePriorities = true;
    protected static boolean debug = false;

    protected static void parseArgs(final String[] args) {
        int i = 0;
        while (i < args.length) {
            final String loopOptionKey = args[i];

            switch (loopOptionKey) {
                case "-numChameneos":
                case "-c":
                    i += 1;
                    numChameneos = Integer.parseInt(args[i]);
                    break;
                case "-numMeetings":
                case "-m":
                    i += 1;
                    numMeetings = Integer.parseInt(args[i]);
                    break;
                case "-numChannels":
                case "-numMailboxes":
                case "-nm":
                    i += 1;
                    numMailboxes = Integer.parseInt(args[i]);
                    break;
                case "-p":
                    i += 1;
                    usePriorities = Boolean.parseBoolean(args[i]);
                    break;
                case "-debug":
                case "-verbose":
                    debug = true;
                    break;
            }

            i += 1;
        }
    }

    protected static void printArgs() {
        System.out.printf(BenchmarkRunner.argOutputFormat, "num chameneos", numChameneos);
        System.out.printf(BenchmarkRunner.argOutputFormat, "num meetings", numMeetings);
        System.out.printf(BenchmarkRunner.argOutputFormat, "num mailboxes", numMailboxes);
        System.out.printf(BenchmarkRunner.argOutputFormat, "use priorities", usePriorities);
        System.out.printf(BenchmarkRunner.argOutputFormat, "debug", debug);
    }
}
