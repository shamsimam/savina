package edu.rice.habanero.benchmarks.recmatmul;

import edu.rice.habanero.benchmarks.BenchmarkRunner;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class MatMulConfig {

    protected static int NUM_WORKERS = 20;
    protected static int DATA_LENGTH = 1_024;
    protected static int BLOCK_THRESHOLD = 16_384;
    protected static int PRIORITIES = 10;
    protected static boolean debug = false;

    protected static double[][] A = null;
    protected static double[][] B = null;
    protected static double[][] C = null;

    protected static void parseArgs(final String[] args) {
        int i = 0;
        while (i < args.length) {
            final String loopOptionKey = args[i];
            switch (loopOptionKey) {
                case "-n":
                    i += 1;
                    DATA_LENGTH = Integer.parseInt(args[i]);
                    break;
                case "-t":
                    i += 1;
                    BLOCK_THRESHOLD = Integer.parseInt(args[i]);
                    break;
                case "-w":
                    i += 1;
                    NUM_WORKERS = Integer.parseInt(args[i]);
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

        initializeData();
    }

    protected static void printArgs() {
        System.out.printf(BenchmarkRunner.argOutputFormat, "Num Workers", NUM_WORKERS);
        System.out.printf(BenchmarkRunner.argOutputFormat, "Data length", DATA_LENGTH);
        System.out.printf(BenchmarkRunner.argOutputFormat, "Threshold", BLOCK_THRESHOLD);
        System.out.printf(BenchmarkRunner.argOutputFormat, "Priorities", PRIORITIES);
        System.out.printf(BenchmarkRunner.argOutputFormat, "debug", debug);
    }

    protected static void initializeData() {

        A = null;
        B = null;
        C = null;

        System.gc();
        System.gc();
        System.gc();

        A = new double[DATA_LENGTH][DATA_LENGTH];
        B = new double[DATA_LENGTH][DATA_LENGTH];
        C = new double[DATA_LENGTH][DATA_LENGTH];

        for (int i = 0; i < DATA_LENGTH; i++) {
            for (int j = 0; j < DATA_LENGTH; j++) {
                A[i][j] = i;
                B[i][j] = j;
            }
        }
    }

    protected static boolean valid() {
        for (int i = 0; i < DATA_LENGTH; i++) {
            for (int j = 0; j < DATA_LENGTH; j++) {
                final double actual = C[i][j];
                final double expected = 1.0 * DATA_LENGTH * i * j;
                if (Double.compare(actual, expected) != 0) {
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
        public final int srA;
        public final int scA;
        public final int srB;
        public final int scB;
        public final int srC;
        public final int scC;
        public final int numBlocks;
        public final int dim;

        public WorkMessage(
                final int priority,
                final int srA, final int scA,
                final int srB, final int scB,
                final int srC, final int scC,
                final int numBlocks, final int dim) {
            this.priority = priority;
            this.srA = srA;
            this.scA = scA;
            this.srB = srB;
            this.scB = scB;
            this.srC = srC;
            this.scC = scC;
            this.numBlocks = numBlocks;
            this.dim = dim;
        }
    }

    protected static class DoneMessage {
        protected static DoneMessage ONLY = new DoneMessage();

        private DoneMessage() {
            super();
        }
    }

    protected static class StopMessage {
        protected static StopMessage ONLY = new StopMessage();

        private StopMessage() {
            super();
        }
    }
}
