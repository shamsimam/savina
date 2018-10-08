package edu.rice.habanero.benchmarks.trapezoid;

import edu.rice.habanero.benchmarks.BenchmarkRunner;
import edu.rice.habanero.benchmarks.CliArgumentParser;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class TrapezoidalConfig {

    protected static int N = 10_000_000; // num pieces
    protected static int W = 100; // num workers
    protected static double L = 1; // left end-point
    protected static double R = 5; // right end-point
    protected static boolean debug = false;

    protected static void parseArgs(final String[] args) {
        CliArgumentParser ap = new CliArgumentParser(args);
        N = ap.getIntValue(new String[] {"-n"}, N);
        W = ap.getIntValue(new String[] {"-w"}, W);
        L = ap.getDoubleValue(new String[] {"-l"}, L);
        R = ap.getDoubleValue(new String[] {"-r"}, R);
        debug = ap.getBoolValue(new String[] {"--debug", "--verbose"}, debug);
    }

    protected static void printArgs() {
        System.out.printf(BenchmarkRunner.argOutputFormat, "N (num trapezoids)", N);
        System.out.printf(BenchmarkRunner.argOutputFormat, "W (num workers)", W);
        System.out.printf(BenchmarkRunner.argOutputFormat, "L (left end-point)", L);
        System.out.printf(BenchmarkRunner.argOutputFormat, "R (right end-point)", R);
        System.out.printf(BenchmarkRunner.argOutputFormat, "debug", debug);
    }

    protected static double fx(final double x) {

        final double a = Math.sin(Math.pow(x, 3) - 1);
        final double b = x + 1;
        final double c = a / b;
        final double d = Math.sqrt(1 + Math.exp(Math.sqrt(2 * x)));
        final double r = c * d;
        return r;
    }

    protected static final class WorkMessage {
        final double l;
        final double r;
        final double h;

        public WorkMessage(final double l, final double r, final double h) {
            this.l = l;
            this.r = r;
            this.h = h;
        }
    }

    protected static final class ResultMessage {
        public final double result;
        public final int workerId;

        public ResultMessage(final double result, final int workerId) {
            this.result = result;
            this.workerId = workerId;
        }
    }
}
