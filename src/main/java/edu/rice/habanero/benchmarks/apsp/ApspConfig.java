package edu.rice.habanero.benchmarks.apsp;

import edu.rice.habanero.benchmarks.BenchmarkRunner;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class ApspConfig {

    protected static int N = 300;
    protected static int B = 50;
    protected static int W = 100;
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
                case "-b":
                    i += 1;
                    B = Integer.parseInt(args[i]);
                    break;
                case "-w":
                    i += 1;
                    W = Integer.parseInt(args[i]);
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
        System.out.printf(BenchmarkRunner.argOutputFormat, "B (block size)", B);
        System.out.printf(BenchmarkRunner.argOutputFormat, "W (max edge weight)", W);
        System.out.printf(BenchmarkRunner.argOutputFormat, "debug", debug);
    }
}
