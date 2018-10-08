package edu.rice.habanero.benchmarks.cigsmok;

import edu.rice.habanero.benchmarks.BenchmarkRunner;
import edu.rice.habanero.benchmarks.CliArgumentParser;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class CigaretteSmokerConfig {

    protected static int R = 1_000; // num rounds
    protected static int S = 200; // num smokers / ingredients
    protected static boolean debug = false;

    protected static void parseArgs(final String[] args) {
        CliArgumentParser ap = new CliArgumentParser(args);
        R = ap.getIntValue(new String[] {"-r"}, R);
        S = ap.getIntValue(new String[] {"-s"}, S);
        debug = ap.getBoolValue(new String[] {"--debug", "--verbose"}, debug);
    }

    protected static void printArgs() {
        System.out.printf(BenchmarkRunner.argOutputFormat, "R (num rounds)", R);
        System.out.printf(BenchmarkRunner.argOutputFormat, "S (num smokers)", S);
        System.out.printf(BenchmarkRunner.argOutputFormat, "debug", debug);
    }

    protected static int busyWait(final int limit) {
        int test = 0;

        for (int k = 0; k < limit; k++) {
            Math.random();
            test++;
        }

        return test;
    }

    protected static class StartSmoking {
        public final int busyWaitPeriod;

        public StartSmoking(final int busyWaitPeriod) {
            this.busyWaitPeriod = busyWaitPeriod;
        }
    }

    protected static class StartedSmoking {
        public static StartedSmoking ONLY = new StartedSmoking();
    }

    protected static class StartMessage {
        public static StartMessage ONLY = new StartMessage();
    }

    protected static class ExitMessage {
        public static ExitMessage ONLY = new ExitMessage();
    }
}
