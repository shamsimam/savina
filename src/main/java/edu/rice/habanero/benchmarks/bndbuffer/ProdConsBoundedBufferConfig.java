package edu.rice.habanero.benchmarks.bndbuffer;

import edu.rice.habanero.benchmarks.BenchmarkRunner;
import edu.rice.habanero.benchmarks.PseudoRandom;

/**
 * Computes Logistic Map source: http://en.wikipedia.org/wiki/Logistic_map
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class ProdConsBoundedBufferConfig {

    protected static int bufferSize = 50;
    protected static int numProducers = 40;
    protected static int numConsumers = 40;
    protected static int numItemsPerProducer = 1_000;
    protected static int prodCost = 25;
    protected static int consCost = 25;
    protected static int numMailboxes = 1;
    protected static boolean debug = false;

    protected static void parseArgs(final String[] args) {
        int i = 0;
        while (i < args.length) {
            final String loopOptionKey = args[i];

            switch (loopOptionKey) {
                case "-bb":
                    i += 1;
                    bufferSize = Integer.parseInt(args[i]);
                    break;
                case "-np":
                    i += 1;
                    numProducers = Integer.parseInt(args[i]);
                    break;
                case "-nc":
                    i += 1;
                    numConsumers = Integer.parseInt(args[i]);
                    break;
                case "-pc":
                    i += 1;
                    prodCost = Integer.parseInt(args[i]);
                    break;
                case "-cc":
                    i += 1;
                    consCost = Integer.parseInt(args[i]);
                    break;
                case "-ipp":
                    i += 1;
                    numItemsPerProducer = Integer.parseInt(args[i]);
                    break;
                case "-numChannels":
                case "-numMailboxes":
                case "-nm":
                    i += 1;
                    numMailboxes = Integer.parseInt(args[i]);
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
        System.out.printf(BenchmarkRunner.argOutputFormat, "Buffer size", bufferSize);
        System.out.printf(BenchmarkRunner.argOutputFormat, "num producers", numProducers);
        System.out.printf(BenchmarkRunner.argOutputFormat, "num consumers", numConsumers);
        System.out.printf(BenchmarkRunner.argOutputFormat, "prod cost", prodCost);
        System.out.printf(BenchmarkRunner.argOutputFormat, "cons cost", consCost);
        System.out.printf(BenchmarkRunner.argOutputFormat, "items per producer", numItemsPerProducer);
        System.out.printf(BenchmarkRunner.argOutputFormat, "num mailboxes", numMailboxes);
        System.out.printf(BenchmarkRunner.argOutputFormat, "debug", debug);
    }

    protected static double processItem(final double curTerm, final int cost) {
        double res = curTerm;

        final PseudoRandom random = new PseudoRandom(cost);
        if (cost > 0) {
            for (int i = 0; i < cost; i++) {
                for (int j = 0; j < 100; j++) {
                    res += Math.log(Math.abs(random.nextDouble()) + 0.01);
                }
            }
        } else {
            res += Math.log(Math.abs(random.nextDouble()) + 0.01);
        }

        return res;
    }

    protected enum MessageSource {
        PRODUCER,
        CONSUMER
    }

    protected static class DataItemMessage {
        public final double data;
        public final Object producer;

        DataItemMessage(final double data, final Object producer) {
            this.data = data;
            this.producer = producer;
        }
    }

    protected static class ProduceDataMessage {
        protected static ProduceDataMessage ONLY = new ProduceDataMessage();
    }

    protected static class ProducerExitMessage {
        protected static ProducerExitMessage ONLY = new ProducerExitMessage();
    }

    protected static class ConsumerAvailableMessage {
        public final Object consumer;

        ConsumerAvailableMessage(final Object consumer) {
            this.consumer = consumer;
        }
    }

    protected static class ConsumerExitMessage {
        protected static ConsumerExitMessage ONLY = new ConsumerExitMessage();
    }
}
