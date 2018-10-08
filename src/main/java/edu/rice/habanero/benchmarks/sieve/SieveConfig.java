package edu.rice.habanero.benchmarks.sieve;

import edu.rice.habanero.benchmarks.BenchmarkRunner;
import edu.rice.habanero.benchmarks.CliArgumentParser;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class SieveConfig {

    protected static long N = 100_000;
    protected static int M = 1_000;
    protected static boolean debug = false;

    protected static void parseArgs(final String[] args) {
        CliArgumentParser ap = new CliArgumentParser(args);
        N = ap.getLongValue(new String[] {"-n"}, N);
        M = ap.getIntValue(new String[] {"-m"}, M);
        debug = ap.getBoolValue(new String[] {"--debug", "--verbose"}, debug);
    }

    protected static void printArgs() {
        System.out.printf(BenchmarkRunner.argOutputFormat, "N (input size)", N);
        System.out.printf(BenchmarkRunner.argOutputFormat, "debug", debug);
    }

    protected static boolean isLocallyPrime(
            final long candidate,
            final long[] localPrimes,
            final int startInc,
            final int endExc) {

        for (int i = startInc; i < endExc; i++) {
            final long remainder = candidate % localPrimes[i];
            if (remainder == 0) {
                return false;
            }
        }
        return true;
    }
}
