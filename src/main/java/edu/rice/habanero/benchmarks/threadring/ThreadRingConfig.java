package edu.rice.habanero.benchmarks.threadring;

import edu.rice.habanero.benchmarks.BenchmarkRunner;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class ThreadRingConfig {

    protected static int N = 100; // num actors
    protected static int R = 100_000; // num pings, does not need to be divisible by N
    protected static boolean debug = false;

    protected static void parseArgs(final String[] args) {
        int i = 0;
        while (i < args.length) {
            final String loopOptionKey = args[i];
            if ("-n".equals(loopOptionKey)) {
                i += 1;
                N = Integer.parseInt(args[i]);
            } else if ("-r".equals(loopOptionKey)) {
                i += 1;
                R = Integer.parseInt(args[i]);
            } else if ("-debug".equals(loopOptionKey) || "-verbose".equals(loopOptionKey)) {
                debug = true;
            }
            i += 1;
        }
    }

    protected static void printArgs() {
        System.out.printf(BenchmarkRunner.argOutputFormat, "N (num actors)", N);
        System.out.printf(BenchmarkRunner.argOutputFormat, "R (num rounds)", R);
        System.out.printf(BenchmarkRunner.argOutputFormat, "debug", debug);
    }

    protected static final class PingMessage {
        public final int pingsLeft;

        protected PingMessage(final int pingsLeft) {
            this.pingsLeft = pingsLeft;
        }

        protected boolean hasNext() {
            return pingsLeft > 0;
        }

        protected PingMessage next() {
            return new PingMessage(pingsLeft - 1);
        }
    }

    protected static final class DataMessage {
        public final Object data;

        protected DataMessage(final Object data) {
            this.data = data;
        }
    }

    protected static final class ExitMessage {
        public final int exitsLeft;

        protected ExitMessage(final int exitsLeft) {
            this.exitsLeft = exitsLeft;
        }

        protected boolean hasNext() {
            return exitsLeft > 0;
        }

        protected ExitMessage next() {
            return new ExitMessage(exitsLeft - 1);
        }
    }
}
