package edu.rice.habanero.benchmarks.fjthrput;

import edu.rice.habanero.benchmarks.BenchmarkRunner;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class ThroughputConfig {

    protected static int N = 10_000;
    protected static int A = 60;
    protected static int C = 1;
    protected static boolean usePriorities = true;
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
                case "-a":
                    i += 1;
                    A = Integer.parseInt(args[i]);
                    break;
                case "-c":
                    i += 1;
                    C = Integer.parseInt(args[i]);
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
        System.out.printf(BenchmarkRunner.argOutputFormat, "N (messages per actor)", N);
        System.out.printf(BenchmarkRunner.argOutputFormat, "A (num actors)", A);
        System.out.printf(BenchmarkRunner.argOutputFormat, "C (num channels)", C);
        System.out.printf(BenchmarkRunner.argOutputFormat, "P (use priorities)", usePriorities);
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
