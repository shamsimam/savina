package edu.rice.habanero.benchmarks.count;

import edu.rice.habanero.benchmarks.BenchmarkRunner;
import edu.rice.habanero.benchmarks.CliArgumentParser;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class CountingConfig {

    protected static int N = 1_000_000;
    protected static boolean debug = false;

    protected static void parseArgs(final String[] args) {
        CliArgumentParser ap = new CliArgumentParser(args);
        N = ap.getIntValue(new String[] {"-n"}, N);
        debug = ap.getBoolValue(new String[] {"--debug", "--verbose"}, debug);
    }

    protected static void printArgs() {
        System.out.printf(BenchmarkRunner.argOutputFormat, "N (num messages)", N);
        System.out.printf(BenchmarkRunner.argOutputFormat, "debug", debug);
    }
}
