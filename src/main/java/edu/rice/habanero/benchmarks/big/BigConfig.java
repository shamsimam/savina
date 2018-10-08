package edu.rice.habanero.benchmarks.big;

import edu.rice.habanero.benchmarks.BenchmarkRunner;
import edu.rice.habanero.benchmarks.CliArgumentParser;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class BigConfig {

    protected static int N = 20_000; // num pings
    protected static int W = 120; // num actors
    protected static boolean debug = false;

    protected static void parseArgs(final String[] args) {
        CliArgumentParser ap = new CliArgumentParser(args);
        N = ap.getIntValue(new String[] {"-n"}, N);
        W = ap.getIntValue(new String[] {"-w"}, W);
        debug = ap.getBoolValue(new String[] {"--debug", "--verbose"}, debug);
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
