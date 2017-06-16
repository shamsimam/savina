package edu.rice.habanero.benchmarks.philosopher;

import edu.rice.habanero.benchmarks.BenchmarkRunner;
import edu.rice.habanero.benchmarks.CliArgumentParser;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class PhilosopherConfig {

    protected static int N = 20; // num philosophers
    protected static int M = 10_000; // num eating rounds
    protected static int C = 1; // num channels
    protected static boolean debug = false;

    protected static void parseArgs(final String[] args) {
        CliArgumentParser ap = new CliArgumentParser(args);
        N = ap.getIntValue(new String[] {"-n"}, N);
        M = ap.getIntValue(new String[] {"-m"}, M);
        C = ap.getIntValue(new String[] {"-c"}, C);
        debug = ap.getBoolValue(new String[] {"--debug", "--verbose"}, debug);
    }

    protected static void printArgs() {
        System.out.printf(BenchmarkRunner.argOutputFormat, "N (num philosophers)", N);
        System.out.printf(BenchmarkRunner.argOutputFormat, "M (num eating rounds)", M);
        System.out.printf(BenchmarkRunner.argOutputFormat, "C (num channels)", C);
        System.out.printf(BenchmarkRunner.argOutputFormat, "debug", debug);
    }
}
