package edu.rice.habanero.benchmarks.radixsort;

import edu.rice.habanero.benchmarks.BenchmarkRunner;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class RadixSortConfig {

    protected static int N = 100_000; // data size
    protected static long M = 1L << 60; // max value
    protected static long S = 2_048; // seed for random number generator
    protected static boolean debug = false;

    protected static void parseArgs(final String[] args) {
        int i = 0;
        while (i < args.length) {
            final String loopOptionKey = args[i];
            switch (loopOptionKey) {
                case "-n":
                    i += 1;
                    N = Integer.parseInt(args[i]);
                    break;
                case "-m":
                    i += 1;
                    M = Long.parseLong(args[i]);
                    break;
                case "-s":
                    i += 1;
                    S = Long.parseLong(args[i]);
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
        System.out.printf(BenchmarkRunner.argOutputFormat, "N (num values)", N);
        System.out.printf(BenchmarkRunner.argOutputFormat, "M (max value)", M);
        System.out.printf(BenchmarkRunner.argOutputFormat, "S (seed)", S);
        System.out.printf(BenchmarkRunner.argOutputFormat, "debug", debug);
    }
}
