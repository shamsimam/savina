package edu.rice.habanero.benchmarks.philosopher;

import edu.rice.habanero.benchmarks.BenchmarkRunner;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class PhilosopherConfig {

    protected static int N = 20; // num philosophers
    protected static int M = 10_000; // num eating rounds
    protected static int C = 1; // num channels
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
                    M = Integer.parseInt(args[i]);
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
        System.out.printf(BenchmarkRunner.argOutputFormat, "N (num philosophers)", N);
        System.out.printf(BenchmarkRunner.argOutputFormat, "M (num eating rounds)", M);
        System.out.printf(BenchmarkRunner.argOutputFormat, "C (num channels)", C);
        System.out.printf(BenchmarkRunner.argOutputFormat, "debug", debug);
    }
}
