package edu.rice.habanero.benchmarks.chameneos;

import edu.rice.habanero.benchmarks.BenchmarkRunner;
import edu.rice.habanero.benchmarks.CliArgumentParser;

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
        CliArgumentParser ap = new CliArgumentParser(args);
        numChameneos = ap.getIntValue(new String[] {"-numChameneos", "-c"}, numChameneos);
        numMeetings = ap.getIntValue(new String[] {"-numMeetings", "-m"}, numMeetings);
        numMailboxes = ap.getIntValue(new String[] {"-numChannels", "-numMailboxes", "-nm"}, numMailboxes);
        usePriorities = ap.getBoolValue(new String[] {"-p"}, usePriorities);
        debug = ap.getBoolValue(new String[] {"--debug", "--verbose"}, debug);
    }

    protected static void printArgs() {
        System.out.printf(BenchmarkRunner.argOutputFormat, "num chameneos", numChameneos);
        System.out.printf(BenchmarkRunner.argOutputFormat, "num meetings", numMeetings);
        System.out.printf(BenchmarkRunner.argOutputFormat, "num mailboxes", numMailboxes);
        System.out.printf(BenchmarkRunner.argOutputFormat, "use priorities", usePriorities);
        System.out.printf(BenchmarkRunner.argOutputFormat, "debug", debug);
    }
}
