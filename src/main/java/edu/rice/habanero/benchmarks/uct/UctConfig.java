package edu.rice.habanero.benchmarks.uct;

import edu.rice.habanero.benchmarks.BenchmarkRunner;

/**
 * Unbalanced Cobwebbed Tree benchmark.
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class UctConfig {

    protected static int MAX_NODES = 200_000; //_000; // maximum nodes
    protected static int AVG_COMP_SIZE = 500; // average computation size
    protected static int STDEV_COMP_SIZE = 100; // standard deviation of the computation size
    protected static int BINOMIAL_PARAM = 10; // binomial parameter: each node may have either 0 or binomial children
    protected static int URGENT_NODE_PERCENT = 50; // percentage of urgent nodes
    protected static boolean debug = false;

    protected static void parseArgs(final String[] args) {
        int i = 0;
        while (i < args.length) {
            final String loopOptionKey = args[i];

            switch (loopOptionKey) {
                case "-nodes":
                    i += 1;
                    MAX_NODES = Integer.parseInt(args[i]);
                    break;
                case "-avg":
                    i += 1;
                    AVG_COMP_SIZE = Integer.parseInt(args[i]);
                    break;
                case "-stdev":
                    i += 1;
                    STDEV_COMP_SIZE = Integer.parseInt(args[i]);
                    break;
                case "-binomial":
                    i += 1;
                    BINOMIAL_PARAM = Integer.parseInt(args[i]);
                    break;
                case "-urgent":
                    i += 1;
                    URGENT_NODE_PERCENT = Integer.parseInt(args[i]);
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
        System.out.printf(BenchmarkRunner.argOutputFormat, "Max. nodes", MAX_NODES);
        System.out.printf(BenchmarkRunner.argOutputFormat, "Avg. comp size", AVG_COMP_SIZE);
        System.out.printf(BenchmarkRunner.argOutputFormat, "Std. dev. comp size", STDEV_COMP_SIZE);
        System.out.printf(BenchmarkRunner.argOutputFormat, "Binomial Param", BINOMIAL_PARAM);
        System.out.printf(BenchmarkRunner.argOutputFormat, "Urgent node percent", URGENT_NODE_PERCENT);
        System.out.printf(BenchmarkRunner.argOutputFormat, "debug", debug);
    }

    protected static int loop(int busywait, int dummy) {
        int test = 0;
        long current = System.currentTimeMillis();

        for (int k = 0; k < dummy * busywait; k++) {
            test++;
        }

        return test;
    }

    protected static class GetIdMessage {
        protected static GetIdMessage ONLY = new GetIdMessage();
    }

    protected static class PrintInfoMessage {
        protected static PrintInfoMessage ONLY = new PrintInfoMessage();
    }

    protected static class GenerateTreeMessage {
        protected static GenerateTreeMessage ONLY = new GenerateTreeMessage();
    }

    protected static class TryGenerateChildrenMessage {
        protected static TryGenerateChildrenMessage ONLY = new TryGenerateChildrenMessage();
    }

    protected static class GenerateChildrenMessage {
        public final int currentId;
        public final int compSize;

        public GenerateChildrenMessage(final int currentId, final int compSize) {
            this.currentId = currentId;
            this.compSize = compSize;
        }
    }

    protected static class UrgentGenerateChildrenMessage {
        public final int urgentChildId;
        public final int currentId;
        public final int compSize;

        public UrgentGenerateChildrenMessage(final int urgentChildId, final int currentId, final int compSize) {
            this.urgentChildId = urgentChildId;
            this.currentId = currentId;
            this.compSize = compSize;
        }
    }

    protected static class TraverseMessage {
        protected static TraverseMessage ONLY = new TraverseMessage();
    }

    protected static class UrgentTraverseMessage {
        protected static UrgentTraverseMessage ONLY = new UrgentTraverseMessage();
    }

    protected static class ShouldGenerateChildrenMessage {
        public final Object sender;
        public final int childHeight;

        public ShouldGenerateChildrenMessage(final Object sender, final int childHeight) {
            this.sender = sender;
            this.childHeight = childHeight;
        }
    }

    protected static class UpdateGrantMessage {
        public final int childId;

        public UpdateGrantMessage(final int childId) {
            this.childId = childId;
        }
    }

    protected static class TerminateMessage {
        protected static TerminateMessage ONLY = new TerminateMessage();
    }
}
