package edu.rice.habanero.benchmarks.sieve;

import edu.rice.habanero.benchmarks.BenchmarkRunner;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class SieveConfig {

    protected static long N = 100_000;
    protected static int M = 1_000;
    protected static boolean debug = false;

    protected static void parseArgs(final String[] args) {
        int i = 0;
        while (i < args.length) {
            final String loopOptionKey = args[i];
            switch (loopOptionKey) {
                case "-n":
                    i += 1;
                    N = Long.parseLong(args[i]);
                    break;
                case "-m":
                    i += 1;
                    M = Integer.parseInt(args[i]);
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
