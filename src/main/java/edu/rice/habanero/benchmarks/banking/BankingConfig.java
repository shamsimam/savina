package edu.rice.habanero.benchmarks.banking;

import edu.rice.habanero.benchmarks.BenchmarkRunner;
import edu.rice.habanero.benchmarks.CliArgumentParser;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class BankingConfig {

    protected static int A = 1_000; // num accounts
    protected static int N = 50_000; // num transactions
    protected static double INITIAL_BALANCE = Double.MAX_VALUE / (A * N);
    protected static boolean debug = false;

    protected static void parseArgs(final String[] args) {
        CliArgumentParser ap = new CliArgumentParser(args);
        A = ap.getIntValue(new String[] {"-a"}, A);
        N = ap.getIntValue(new String[] {"-n"}, N);
        debug = ap.getBoolValue(new String[] {"--debug", "--verbose"}, debug);

        INITIAL_BALANCE = ((Double.MAX_VALUE / (A * N)) / 1_000) * 1_000;
    }

    protected static void printArgs() {
        System.out.printf(BenchmarkRunner.argOutputFormat, "A (num accounts)", A);
        System.out.printf(BenchmarkRunner.argOutputFormat, "N (num transactions)", N);
        System.out.printf(BenchmarkRunner.argOutputFormat, "Initial Balance", INITIAL_BALANCE);
        System.out.printf(BenchmarkRunner.argOutputFormat, "debug", debug);
    }

    protected static final class StartMessage {
        protected static StartMessage ONLY = new StartMessage();
    }

    protected static final class StopMessage {
        protected static StopMessage ONLY = new StopMessage();
    }

    protected static final class ReplyMessage {
        protected static ReplyMessage ONLY = new ReplyMessage();
    }

    protected static class DebitMessage {
        public final Object sender;
        public final double amount;

        public DebitMessage(final Object sender, final double amount) {
            this.sender = sender;
            this.amount = amount;
        }
    }

    protected static class CreditMessage {
        public final Object sender;
        public final double amount;
        public final Object recipient;

        public CreditMessage(final Object sender, final double amount, final Object recipient) {
            this.sender = sender;
            this.amount = amount;
            this.recipient = recipient;
        }
    }
}
