package edu.rice.habanero.benchmarks.concsll;

import edu.rice.habanero.benchmarks.BenchmarkRunner;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class SortedListConfig {

    protected static int NUM_ENTITIES = 20;
    protected static int NUM_MSGS_PER_WORKER = 8_000;
    protected static int WRITE_PERCENTAGE = 10;
    protected static int SIZE_PERCENTAGE = 1;

    protected static boolean debug = false;

    protected static void parseArgs(final String[] args) {
        int i = 0;
        while (i < args.length) {
            final String loopOptionKey = args[i];
            switch (loopOptionKey) {
                case "-e":
                    i += 1;
                    NUM_ENTITIES = Integer.parseInt(args[i]);
                    break;
                case "-m":
                    i += 1;
                    NUM_MSGS_PER_WORKER = Integer.parseInt(args[i]);
                    break;
                case "-w":
                    i += 1;
                    WRITE_PERCENTAGE = Integer.parseInt(args[i]);
                    break;
                case "-s":
                    i += 1;
                    SIZE_PERCENTAGE = Integer.parseInt(args[i]);
                    break;
                case "debug":
                case "verbose":
                    debug = true;
                    break;
            }
            i += 1;
        }

        if (WRITE_PERCENTAGE >= 50) {
            throw new IllegalArgumentException("Write rate must be less than 50!");
        }
        if ((2 * WRITE_PERCENTAGE + SIZE_PERCENTAGE) >= 100) {
            throw new IllegalArgumentException("(2 * write-rate) + sum-rate must be less than 100!");
        }
    }

    protected static void printArgs() {
        System.out.printf(BenchmarkRunner.argOutputFormat, "Num Entities", NUM_ENTITIES);
        System.out.printf(BenchmarkRunner.argOutputFormat, "Message/Worker", NUM_MSGS_PER_WORKER);
        System.out.printf(BenchmarkRunner.argOutputFormat, "Insert Percent", WRITE_PERCENTAGE);
        System.out.printf(BenchmarkRunner.argOutputFormat, "Size Percent", SIZE_PERCENTAGE);
        System.out.printf(BenchmarkRunner.argOutputFormat, "debug", debug);
    }

    protected static class WriteMessage {

        protected final Object sender;
        protected final int value;

        protected WriteMessage(final Object sender, final int value) {
            this.sender = sender;
            this.value = value;
        }
    }

    protected static class ContainsMessage {

        protected final Object sender;
        protected final int value;

        protected ContainsMessage(final Object sender, final int value) {
            this.sender = sender;
            this.value = value;
        }
    }

    protected static class SizeMessage {

        protected final Object sender;

        protected SizeMessage(final Object sender) {
            this.sender = sender;
        }
    }

    protected static class ResultMessage {

        protected final Object sender;
        protected final int value;

        protected ResultMessage(final Object sender, final int value) {
            this.sender = sender;
            this.value = value;
        }
    }

    protected static class DoWorkMessage {

        protected static final DoWorkMessage ONLY = new DoWorkMessage();

        private DoWorkMessage() {
            super();
        }
    }

    protected static class EndWorkMessage {

        protected static final EndWorkMessage ONLY = new EndWorkMessage();

        private EndWorkMessage() {
            super();
        }
    }
}
