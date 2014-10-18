package edu.rice.habanero.benchmarks.filterbank;

import edu.rice.habanero.benchmarks.BenchmarkRunner;

import java.util.Collection;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class FilterBankConfig {

    protected static int NUM_COLUMNS = 8192 * 2;
    protected static int NUM_SIMULATIONS = 2048 + (Math.max(2048, NUM_COLUMNS) * 2);
    protected static int NUM_CHANNELS = 8;
    protected static int SINK_PRINT_RATE = 100;

    protected static boolean debug = false;

    protected static double[][] H = null;
    protected static double[][] F = null;

    protected static void parseArgs(final String[] args) {
        int i = 0;
        while (i < args.length) {
            final String loopOptionKey = args[i];
            switch (loopOptionKey) {
                case "-sim":
                case "-simulation":
                    i += 1;
                    NUM_SIMULATIONS = Integer.parseInt(args[i]);
                    break;
                case "-col":
                case "-columns":
                    i += 1;
                    NUM_COLUMNS = Integer.parseInt(args[i]);
                    break;
                case "-chan":
                case "-channels":
                    i += 1;
                    final int argInt = Integer.parseInt(args[i]);
                    final int maxChannels = MessageChannel.values().length - 1;
                    NUM_CHANNELS = Math.max(2, Math.min(argInt, maxChannels));
                    break;
                case "-debug":
                case "-verbose":
                    debug = true;
                    break;
            }
            i += 1;
        }

        H = new double[NUM_CHANNELS][NUM_COLUMNS];
        F = new double[NUM_CHANNELS][NUM_COLUMNS];
        for (int j = 0; j < NUM_CHANNELS; j++) {
            for (i = 0; i < NUM_COLUMNS; i++) {
                H[j][i] = (1.0 * i * NUM_COLUMNS) + (1.0 * j * NUM_CHANNELS) + j + i + j + 1;
                F[j][i] = (1.0 * i * j) + (1.0 * j * j) + j + i;
            }
        }
    }

    protected static void printArgs() {
        System.out.printf(BenchmarkRunner.argOutputFormat, "numSimulations", NUM_SIMULATIONS);
        System.out.printf(BenchmarkRunner.argOutputFormat, "numColumns", NUM_COLUMNS);
        System.out.printf(BenchmarkRunner.argOutputFormat, "numChannels", NUM_CHANNELS);
        System.out.printf(BenchmarkRunner.argOutputFormat, "debug", debug);
    }

    protected enum MessageChannel {
        C00,
        C01,
        C02,
        C03,
        C04,
        C05,
        C06,
        C07,
        C08,
        C09,
        C10,
        C11,
        C12,
        C13,
        C14,
        C15,
        C16,
        C17,
        C18,
        C19,
        C20,
        C21,
        C22,
        C23,
        C24,
        C25,
        C26,
        C27,
        C28,
        C29,
        C30,
        C31,
        C32,
        C33,
        C_LOWEST
    }

    protected static abstract class Message {
    }

    protected static class NextMessage extends Message {
        public final Object source;

        protected NextMessage(final Object source) {
            this.source = source;
        }
    }

    protected static class BootMessage extends Message {

        protected static final BootMessage ONLY = new BootMessage();

        private BootMessage() {
            super();
        }
    }

    protected static class ExitMessage extends Message {

        protected static final ExitMessage ONLY = new ExitMessage();

        private ExitMessage() {
            super();
        }
    }

    protected static class ValueMessage extends Message {
        public final double value;

        protected ValueMessage(final double value) {
            this.value = value;
        }
    }

    protected static class SourcedValueMessage extends Message {
        public final int sourceId;
        public final double value;

        protected SourcedValueMessage(final int sourceId, final double value) {
            this.sourceId = sourceId;
            this.value = value;
        }
    }

    protected static class CollectionMessage<T> extends Message {
        public final Collection<T> values;

        protected CollectionMessage(final Collection<T> values) {
            this.values = values;
        }
    }
}
