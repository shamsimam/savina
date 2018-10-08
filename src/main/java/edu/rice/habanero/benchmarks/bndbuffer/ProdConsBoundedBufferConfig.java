package edu.rice.habanero.benchmarks.bndbuffer;

import edu.rice.habanero.benchmarks.BenchmarkRunner;
import edu.rice.habanero.benchmarks.CliArgumentParser;
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
        CliArgumentParser ap = new CliArgumentParser(args);
        bufferSize = ap.getIntValue(new String[] {"-bb", "-b"}, bufferSize);
        numProducers = ap.getIntValue(new String[] {"-np", "-p"}, numProducers);
        numConsumers = ap.getIntValue(new String[] {"-nc", "-c"}, numConsumers);
        prodCost = ap.getIntValue(new String[] {"-pc", "-x"}, prodCost);
        consCost = ap.getIntValue(new String[] {"-cc", "-y"}, consCost);
        numItemsPerProducer = ap.getIntValue(new String[] {"-ipp", "-i"}, numItemsPerProducer);
        numMailboxes = ap.getIntValue(new String[] {"-numChannels", "-numMailboxes", "-nm"}, numMailboxes);
        debug = ap.getBoolValue(new String[] {"--debug", "--verbose"}, debug);
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
