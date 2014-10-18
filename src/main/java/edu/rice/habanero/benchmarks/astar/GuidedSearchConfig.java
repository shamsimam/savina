package edu.rice.habanero.benchmarks.astar;

import edu.rice.habanero.benchmarks.BenchmarkRunner;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class GuidedSearchConfig {

    protected static final int PRIORITY_GRANULARITY = 8;
    protected static int NUM_WORKERS = 20;
    protected static int GRID_SIZE = 30;
    protected static int PRIORITIES = 30;
    protected static int THRESHOLD = 1_024;
    protected static boolean debug = false;
    private static Map<Integer, GridNode> allNodes = null;

    protected static void parseArgs(final String[] args) {
        int i = 0;
        while (i < args.length) {
            final String loopOptionKey = args[i];
            switch (loopOptionKey) {
                case "-w":
                    i += 1;
                    NUM_WORKERS = Integer.parseInt(args[i]);
                    break;
                case "-t":
                    i += 1;
                    THRESHOLD = Integer.parseInt(args[i]);
                    break;
                case "-g":
                    i += 1;
                    final int userInput = Integer.parseInt(args[i]);
                    final int allowedMax = (MessagePriority.values().length - 1) * PRIORITY_GRANULARITY;
                    GRID_SIZE = Math.min(userInput, allowedMax);
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
        System.out.printf(BenchmarkRunner.argOutputFormat, "Grid Size", GRID_SIZE);
        System.out.printf(BenchmarkRunner.argOutputFormat, "Priorities", PRIORITIES);
        System.out.printf(BenchmarkRunner.argOutputFormat, "Threshold", THRESHOLD);
        System.out.printf(BenchmarkRunner.argOutputFormat, "Granularity", PRIORITY_GRANULARITY);
        System.out.printf(BenchmarkRunner.argOutputFormat, "debug", debug);
    }

    protected static void initializeData() {
        if (allNodes == null) {
            allNodes = new HashMap<>(GRID_SIZE * GRID_SIZE * GRID_SIZE);

            int id = 0;
            for (int i = 0; i < GRID_SIZE; i++) {
                for (int j = 0; j < GRID_SIZE; j++) {
                    for (int k = 0; k < GRID_SIZE; k++) {
                        final GridNode gridNode = new GridNode(id, i, j, k);
                        allNodes.put(gridNode.id, gridNode);
                        id++;
                    }
                }
            }

            final Random random = new Random(123456L);
            for (final GridNode gridNode : allNodes.values()) {

                int iterCount = 0;
                int neighborCount = 0;
                for (int i = 0; i < 2; i++) {
                    for (int j = 0; j < 2; j++) {
                        for (int k = 0; k < 2; k++) {

                            iterCount++;
                            if (iterCount == 1 || iterCount == 8) {
                                continue;
                            }

                            final boolean addNeighbor = (iterCount == 7 && neighborCount == 0) || random.nextBoolean();
                            if (addNeighbor) {
                                final int newI = Math.min(GRID_SIZE - 1, gridNode.i + i);
                                final int newJ = Math.min(GRID_SIZE - 1, gridNode.j + j);
                                final int newK = Math.min(GRID_SIZE - 1, gridNode.k + k);

                                final int newId = (GRID_SIZE * GRID_SIZE * newI) + (GRID_SIZE * newJ) + newK;
                                final GridNode newNode = allNodes.get(newId);

                                if (gridNode.addNeighbor(newNode)) {
                                    neighborCount++;
                                }
                            }

                        }
                    }
                }
            }
        }

        // clear distance and parent values
        for (final GridNode gridNode : allNodes.values()) {
            gridNode.distanceFromRoot = gridNode.id == 0 ? 0 : -1;
            gridNode.parentInPath.set(null);
        }
    }

    protected static int nodesProcessed() {

        int nodesProcessed = 1;
        for (final GridNode gridNode : allNodes.values()) {
            if (gridNode.parentInPath.get() != null) {
                nodesProcessed++;
            }
        }

        return nodesProcessed;
    }

    protected static boolean validate() {

        GridNode parentNode = targetNode();
        while (parentNode.parentInPath.get() != null) {
            parentNode = parentNode.parentInPath.get();
        }

        final GridNode rootNode = allNodes.get(0);
        return (parentNode == rootNode);
    }

    protected static int priority(final GridNode gridNode) {
        final int availablePriorities = PRIORITIES;
        final int distDiff = Math.max(GRID_SIZE - gridNode.i, Math.max(GRID_SIZE - gridNode.j, GRID_SIZE - gridNode.k));
        final int priorityUnit = distDiff / PRIORITY_GRANULARITY;
        final int resultPriority = Math.abs(availablePriorities - priorityUnit);
        return resultPriority;
    }

    protected static GridNode originNode() {
        return allNodes.get(0);
    }

    protected static GridNode targetNode() {

        final int axisVal = (int) (0.80 * GRID_SIZE);
        final int targetId = (axisVal * GRID_SIZE * GRID_SIZE) + (axisVal * GRID_SIZE) + axisVal;
        final GridNode gridNode = allNodes.get(targetId);
        return gridNode;
    }

    protected static void busyWait() {
        for (int i = 0; i < 100; i++) {
            Math.random();
        }
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
        P30,
        P31,
    }

    protected static final class GridNode {

        public final int id;
        public final int i;
        public final int j;
        public final int k;

        private final List<GridNode> neighbors;
        private final AtomicReference<GridNode> parentInPath;
        // fields used in computing distance
        private int distanceFromRoot;

        public GridNode(final int id, final int i, final int j, final int k) {
            this.id = id;
            this.i = i;
            this.j = j;
            this.k = k;

            this.neighbors = new ArrayList<>();
            distanceFromRoot = id == 0 ? 0 : -1;
            parentInPath = new AtomicReference<>(null);
        }

        private boolean addNeighbor(final GridNode node) {
            if (node == this) {
                return false;
            }
            if (!neighbors.contains(node)) {
                neighbors.add(node);
                return true;
            }
            return false;
        }

        protected int numNeighbors() {
            return neighbors.size();
        }

        protected GridNode neighbor(final int n) {
            return neighbors.get(n);
        }

        protected boolean setParent(final GridNode node) {
            final boolean success = this.parentInPath.compareAndSet(null, node);
            if (success) {
                this.distanceFromRoot = node.distanceFromRoot + (int) distanceFrom(node);
            }
            return success;
        }

        protected double distanceFrom(final GridNode node) {
            final int iDiff = i - node.i;
            final int jDiff = j - node.j;
            final int kDiff = k - node.k;
            return Math.sqrt((iDiff * iDiff) + (jDiff * jDiff) + (kDiff * kDiff));
        }
    }

    protected static class WorkMessage {

        public final GridNode node;
        public final GridNode target;
        public final int priority;

        public WorkMessage(final GridNode node, final GridNode target) {
            this.node = node;
            this.target = target;
            this.priority = priority(node);
        }
    }

    protected static class ReceivedMessage {
        protected static ReceivedMessage ONLY = new ReceivedMessage();

        private ReceivedMessage() {
            super();
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
