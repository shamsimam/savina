package edu.rice.habanero.benchmarks.pingpong;

import edu.rice.habanero.benchmarks.BenchmarkRunner;
import edu.rice.habanero.benchmarks.CliArgumentParser;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class PingPongConfig {

    protected static int N = 40000;
    protected static boolean debug = false;

    protected static void parseArgs(final String[] args) {
        CliArgumentParser ap = new CliArgumentParser(args);
        N = ap.getIntValue(new String[] {"-n"}, N);
        debug = ap.getBoolValue(new String[] {"--debug", "--verbose"}, debug);
    }

    protected static void printArgs() {
        System.out.printf(BenchmarkRunner.argOutputFormat, "N (num pings)", N);
        System.out.printf(BenchmarkRunner.argOutputFormat, "debug", debug);
    }

    protected static abstract class Message {
    }

    protected static class StartMessage extends Message {
        static StartMessage ONLY = new StartMessage();

        protected StartMessage() {
        }
    }

    protected static class PingMessage extends Message {
        static PingMessage ONLY = new PingMessage();

        protected PingMessage() {
        }
    }

    protected static class SendPongMessage extends Message {
        public final Object sender;

        protected SendPongMessage(final Object sender) {
            this.sender = sender;
        }
    }

    protected static class SendPingMessage extends Message {
        public final Object sender;

        protected SendPingMessage(final Object sender) {
            this.sender = sender;
        }
    }

    protected static class StopMessage extends Message {
        protected static StopMessage ONLY = new StopMessage();

        private StopMessage() {
        }
    }
}
