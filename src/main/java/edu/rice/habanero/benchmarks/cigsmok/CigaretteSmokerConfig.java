package edu.rice.habanero.benchmarks.cigsmok;

import edu.rice.habanero.benchmarks.BenchmarkRunner;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class CigaretteSmokerConfig {

    protected static int R = 1_000; // num rounds
    protected static int S = 200; // num smokers / ingredients
    protected static boolean debug = false;

    protected static void parseArgs(final String[] args) {
        int i = 0;
        while (i < args.length) {
            final String loopOptionKey = args[i];
            if ("-r".equals(loopOptionKey)) {
                i += 1;
                R = Integer.parseInt(args[i]);
            } else if ("-s".equals(loopOptionKey)) {
                i += 1;
                S = Integer.parseInt(args[i]);
            } else if ("-debug".equals(loopOptionKey) || "-verbose".equals(loopOptionKey)) {
                debug = true;
            }
            i += 1;
        }
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
