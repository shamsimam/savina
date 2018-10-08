package edu.rice.habanero.benchmarks.fjcreate;

import edu.rice.habanero.benchmarks.BenchmarkRunner;
import edu.rice.habanero.benchmarks.CliArgumentParser;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class ForkJoinConfig {

    protected static int N = 40000;
    protected static int C = 1;
    protected static boolean debug = false;

    protected static void parseArgs(final String[] args) {
        CliArgumentParser ap = new CliArgumentParser(args);
        N = ap.getIntValue(new String[] {"-n"}, N);
        C = ap.getIntValue(new String[] {"-c"}, C);
        debug = ap.getBoolValue(new String[] {"--debug", "--verbose"}, debug);
    }

    protected static void printArgs() {
        System.out.printf(BenchmarkRunner.argOutputFormat, "N (num workers)", N);
        System.out.printf(BenchmarkRunner.argOutputFormat, "C (num channels)", C);
        System.out.printf(BenchmarkRunner.argOutputFormat, "debug", debug);
    }

    protected static void performComputation(final double theta) {
        final double sint = Math.sin(theta);
        final double res = sint * sint;
        //defeat dead code elimination
        if (res <= 0) {
            throw new IllegalStateException("Benchmark exited with unrealistic res value " + res);
        }
    }
}
