package edu.rice.habanero.benchmarks.logmap;

import edu.rice.habanero.benchmarks.BenchmarkRunner;

/**
 * Computes Logistic Map source: http://en.wikipedia.org/wiki/Logistic_map
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class LogisticMapConfig {

    protected static int numTerms = 25_000;
    protected static int numSeries = 10;
    protected static double startRate = 3.46;
    protected static double increment = 0.0025;
    protected static boolean debug = false;

    protected static void parseArgs(final String[] args) {
        int i = 0;
        while (i < args.length) {
            final String loopOptionKey = args[i];

            switch (loopOptionKey) {
                case "-t":
                    i += 1;
                    numTerms = Integer.parseInt(args[i]);
                    break;
                case "-s":
                    i += 1;
                    numSeries = Integer.parseInt(args[i]);
                    break;
                case "-r":
                    i += 1;
                    startRate = Double.parseDouble(args[i]);
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
        System.out.printf(BenchmarkRunner.argOutputFormat, "num terms", numTerms);
        System.out.printf(BenchmarkRunner.argOutputFormat, "num series", numSeries);
        System.out.printf(BenchmarkRunner.argOutputFormat, "start rate", startRate);
        System.out.printf(BenchmarkRunner.argOutputFormat, "debug", debug);
    }

    protected static double computeNextTerm(final double curTerm, final double rate) {
        return rate * curTerm * (1 - curTerm);
    }

    protected static final class StartMessage {
        protected static StartMessage ONLY = new StartMessage();
    }

    protected static final class StopMessage {
        protected static StopMessage ONLY = new StopMessage();
    }

    protected static final class NextTermMessage {
        protected static NextTermMessage ONLY = new NextTermMessage();
    }

    protected static final class GetTermMessage {
        protected static GetTermMessage ONLY = new GetTermMessage();
    }

    protected static class ComputeMessage {
        public final Object sender;
        public final double term;

        public ComputeMessage(final Object sender, final double term) {
            this.sender = sender;
            this.term = term;
        }
    }

    protected static class ResultMessage {
        public final double term;

        public ResultMessage(final double term) {
            this.term = term;
        }
    }
}
