package edu.rice.habanero.benchmarks.piprecision;

import edu.rice.habanero.benchmarks.BenchmarkRunner;
import edu.rice.habanero.benchmarks.CliArgumentParser;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class PiPrecisionConfig {

    private static final BigDecimal one = BigDecimal.ONE;
    private static final BigDecimal two = new BigDecimal(2);
    private static final BigDecimal four = new BigDecimal(4);
    private static final BigDecimal sixteen = new BigDecimal(16);
    protected static int NUM_WORKERS = 20;
    protected static int PRECISION = 5_000;
    protected static boolean debug = false;

    protected static void parseArgs(final String[] args) {
        CliArgumentParser ap = new CliArgumentParser(args);
        NUM_WORKERS = ap.getIntValue(new String[] {"-w"}, NUM_WORKERS);
        PRECISION = ap.getIntValue(new String[] {"-p"}, PRECISION);
        debug = ap.getBoolValue(new String[] {"--debug", "--verbose"}, debug);
    }

    protected static void printArgs() {
        System.out.printf(BenchmarkRunner.argOutputFormat, "Num Workers", NUM_WORKERS);
        System.out.printf(BenchmarkRunner.argOutputFormat, "Precision", PRECISION);
        System.out.printf(BenchmarkRunner.argOutputFormat, "debug", debug);
    }

    /**
     * Original Source Code: http://research.cs.queensu.ca/home/cmpe212/Fall2011/Lab6/Lab6.java Formula:
     * http://mathworld.wolfram.com/BBPFormula.html
     */
    protected static BigDecimal calculateBbpTerm(final int scale, final int k) {
        final RoundingMode roundMode = RoundingMode.HALF_EVEN;

        final int eightK = 8 * k;
        BigDecimal term = four.divide(new BigDecimal(eightK + 1), scale, roundMode);
        term = term.subtract(two.divide(new BigDecimal(eightK + 4), scale, roundMode));
        term = term.subtract(one.divide(new BigDecimal(eightK + 5), scale, roundMode));
        term = term.subtract(one.divide(new BigDecimal(eightK + 6), scale, roundMode));
        term = term.divide(sixteen.pow(k), scale, roundMode);
        return term;
    }

    // Message classes
    protected static class StartMessage {
        protected static StartMessage ONLY = new StartMessage();
    }

    protected static class StopMessage {
        protected static StopMessage ONLY = new StopMessage();
    }

    protected static class WorkMessage {
        public final int scale;
        public final int term;

        public WorkMessage(final int scale, final int term) {
            this.scale = scale;
            this.term = term;
        }
    }

    protected static class ResultMessage {
        public final BigDecimal result;
        public final int workerId;

        public ResultMessage(final BigDecimal result, final int workerId) {
            this.result = result;
            this.workerId = workerId;
        }
    }
}
