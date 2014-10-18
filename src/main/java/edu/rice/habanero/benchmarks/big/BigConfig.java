package edu.rice.habanero.benchmarks.big;

import edu.rice.habanero.benchmarks.BenchmarkRunner;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class BigConfig {

    protected static int N = 20_000; // num pings
    protected static int W = 120; // num actors
    protected static boolean debug = false;

    protected static void parseArgs(final String[] args) {
        int i = 0;
        while (i < args.length) {
            final String loopOptionKey = args[i];
            if ("-n".equals(loopOptionKey)) {
                i += 1;
                N = Integer.parseInt(args[i]);
            } else if ("-w".equals(loopOptionKey)) {
                i += 1;
                W = Integer.parseInt(args[i]);
            } else if ("-debug".equals(loopOptionKey) || "-verbose".equals(loopOptionKey)) {
                debug = true;
            }
            i += 1;
        }
    }

    protected static void printArgs() {
        System.out.printf(BenchmarkRunner.argOutputFormat, "N (num pings)", N);
        System.out.printf(BenchmarkRunner.argOutputFormat, "W (num actors)", W);
        System.out.printf(BenchmarkRunner.argOutputFormat, "debug", debug);
    }

    protected static interface Message {
    }

    protected static final class PingMessage implements Message {
        public final int sender;

        protected PingMessage(final int sender) {
            this.sender = sender;
        }
    }

    protected static final class PongMessage implements Message {
        public final int sender;

        protected PongMessage(final int sender) {
            this.sender = sender;
        }
    }

    protected static final class ExitMessage implements Message {
        public static final ExitMessage ONLY = new ExitMessage();

        protected ExitMessage() {
            super();
        }
    }
}
