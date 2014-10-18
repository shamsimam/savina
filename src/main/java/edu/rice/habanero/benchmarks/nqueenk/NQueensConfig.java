package edu.rice.habanero.benchmarks.nqueenk;

import edu.rice.habanero.benchmarks.BenchmarkRunner;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class NQueensConfig {

    protected static final long[] SOLUTIONS = {
            1,
            0,
            0,
            2,
            10,     /* 5 */
            4,
            40,
            92,
            352,
            724,    /* 10 */
            2680,
            14200,
            73712,
            365596,
            2279184, /* 15 */
            14772512,
            95815104,
            666090624,
            4968057848L,
            39029188884L, /* 20 */
    };
    private static final int MAX_SOLUTIONS = SOLUTIONS.length;

    protected static int NUM_WORKERS = 20;
    protected static int SIZE = 12;
    protected static int THRESHOLD = 4;
    protected static int PRIORITIES = 10;
    protected static int SOLUTIONS_LIMIT = 1_500_000;
    protected static boolean debug = false;

    protected static void parseArgs(final String[] args) {
        int i = 0;
        while (i < args.length) {
            final String loopOptionKey = args[i];
            switch (loopOptionKey) {
                case "-n":
                    i += 1;
                    SIZE = Math.max(1, Math.min(Integer.parseInt(args[i]), MAX_SOLUTIONS));
                    break;
                case "-t":
                    i += 1;
                    THRESHOLD = Math.max(1, Math.min(Integer.parseInt(args[i]), MAX_SOLUTIONS));
                    break;
                case "-w":
                    i += 1;
                    NUM_WORKERS = Integer.parseInt(args[i]);
                    break;
                case "-s":
                    i += 1;
                    SOLUTIONS_LIMIT = Integer.parseInt(args[i]);
                    break;
                case "-p":
                    i += 1;
                    final int priority = Integer.parseInt(args[i]);
                    final int maxPriority = MessagePriority.values().length - 1;
                    PRIORITIES = Math.max(1, Math.min(priority, maxPriority));
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
        System.out.printf(BenchmarkRunner.argOutputFormat, "Num Workers", NUM_WORKERS);
        System.out.printf(BenchmarkRunner.argOutputFormat, "Size", SIZE);
        System.out.printf(BenchmarkRunner.argOutputFormat, "Max Solutions", SOLUTIONS_LIMIT);
        System.out.printf(BenchmarkRunner.argOutputFormat, "Threshold", THRESHOLD);
        System.out.printf(BenchmarkRunner.argOutputFormat, "Priorities", PRIORITIES);
        System.out.printf(BenchmarkRunner.argOutputFormat, "debug", debug);
    }

    /*
     * <a> contains array of <n> queen positions.  Returns 1
     * if none of the queens conflict, and returns 0 otherwise.
     */
    protected static boolean boardValid(final int n, final int[] a) {
        int i, j;
        int p, q;

        for (i = 0; i < n; i++) {
            p = a[i];

            for (j = (i + 1); j < n; j++) {
                q = a[j];
                if (q == p || q == p - (j - i) || q == p + (j - i)) {
                    return false;
                }
            }
        }

        return true;
    }

    public enum MessagePriority {
        P00,
        P01,
        P02,
        P03,
        P04,
        P05,
        P06,
        P07,
        P08,
        P09,
        P10,
        P11,
        P12,
        P13,
        P14,
        P15,
        P16,
        P17,
        P18,
        P19,
        P20,
        P21,
        P22,
        P23,
        P24,
        P25,
        P26,
        P27,
        P28,
        P29,
        P_LOWEST
    }

    protected static class WorkMessage {
        public final int priority;
        public final int[] data;
        public final int depth;

        public WorkMessage(final int priority, final int[] data, final int depth) {
            this.priority = Math.min(PRIORITIES - 1, Math.max(0, priority));
            this.data = data;
            this.depth = depth;
        }
    }

    protected static final class DoneMessage {
        protected static DoneMessage ONLY = new DoneMessage();

        private DoneMessage() {
            super();
        }
    }

    protected static final class ResultMessage {
        static ResultMessage ONLY = new ResultMessage();

        protected ResultMessage() {
            super();
        }
    }

    protected static final class StopMessage {
        protected static StopMessage ONLY = new StopMessage();

        private StopMessage() {
            super();
        }
    }
}
