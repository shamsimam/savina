package edu.rice.habanero.benchmarks.fjcreate;

import edu.rice.habanero.benchmarks.BenchmarkRunner;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class ForkJoinConfig {

    protected static int N = 40000;
    protected static int C = 1;
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
                case "-c":
                    i += 1;
                    C = Integer.parseInt(args[i]);
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
