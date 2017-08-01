package edu.rice.habanero.benchmarks.facloc;

import edu.rice.habanero.benchmarks.BenchmarkRunner;
import edu.rice.habanero.benchmarks.PseudoRandom;

import java.util.*;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class FacilityLocationConfig {

    protected static int NUM_POINTS = 100_000;
    protected static double GRID_SIZE = 500.0;
    protected static double F = Math.sqrt(2) * GRID_SIZE;
    protected static double ALPHA = 2.0;
    protected static int CUTOFF_DEPTH = 3;
    protected static long SEED = 123456L;

    protected static int N = 40000;
    protected static int C = 1;
    protected static boolean debug = false;

    protected static void parseArgs(final String[] args) {
        int i = 0;
        while (i < args.length) {
            final String loopOptionKey = args[i];
            if ("-n".equals(loopOptionKey)) {
                i += 1;
                NUM_POINTS = Integer.parseInt(args[i]);
            } else if ("-g".equals(loopOptionKey)) {
                i += 1;
                GRID_SIZE = java.lang.Double.parseDouble(args[i]);
            } else if ("-a".equals(loopOptionKey)) {
                i += 1;
                ALPHA = java.lang.Double.parseDouble(args[i]);
            } else if ("-s".equals(loopOptionKey)) {
                i += 1;
                SEED = java.lang.Long.parseLong(args[i]);
            } else if ("-c".equals(loopOptionKey)) {
                i += 1;
                CUTOFF_DEPTH = Integer.parseInt(args[i]);
            } else if ("-debug".equals(loopOptionKey) || "-verbose".equals(loopOptionKey)) {
                debug = true;
            }
            i += 1;
        }
        F = Math.sqrt(2) * GRID_SIZE;
    }

    protected static void printArgs() {
        System.out.printf(BenchmarkRunner.argOutputFormat, "Num points", NUM_POINTS);
        System.out.printf(BenchmarkRunner.argOutputFormat, "Grid size", GRID_SIZE);
        System.out.printf(BenchmarkRunner.argOutputFormat, "F", F);
        System.out.printf(BenchmarkRunner.argOutputFormat, "Alpha", ALPHA);
        System.out.printf(BenchmarkRunner.argOutputFormat, "Cut-off depth", CUTOFF_DEPTH);
        System.out.printf(BenchmarkRunner.argOutputFormat, "Seed", SEED);
        System.out.printf(BenchmarkRunner.argOutputFormat, "debug", debug);
    }

    protected static class Point {

        private static PseudoRandom r = new PseudoRandom();

        protected static void setSeed(final long seed) {
            r = new PseudoRandom(seed);
        }

        protected static Point random(final double gridSize) {
            final double x = r.nextDouble() * gridSize;
            final double y = r.nextDouble() * gridSize;
            return new Point(x, y);
        }

        protected static Point findCenter(final Collection<Point> points) {
            double sumX = 0, sumY = 0;

            for (final Point point : points) {
                sumX += point.x;
                sumY += point.y;
            }

            final int numPoints = points.size();
            final double avgX = sumX / numPoints;
            final double avgY = sumY / numPoints;
            return new Point(avgX, avgY);
        }

        protected final double x;
        protected final double y;

        public Point(final double x, final double y) {
            this.x = x;
            this.y = y;
        }

        public double getDistance(final Point p) {
            final double xDiff = p.x - x;
            final double yDiff = p.y - y;
            final double distance = Math.sqrt((xDiff * xDiff) + (yDiff * yDiff));
            return distance;
        }

        @Override
        public String toString() {
            return String.format("(%5.2f, %5.2f)", x, y);
        }
    }

    protected static class Facility {

        public static List<Facility> split(final Facility f) {
            // split points into four categories
            @SuppressWarnings("unchecked")
            final Set<Point>[] pointSplit = new Set[]{
                    new HashSet<Point>(),
                    new HashSet<Point>(),
                    new HashSet<Point>(),
                    new HashSet<Point>()
            };
            final Point tempCenter = Point.findCenter(f.points);
            for (final Point loopPoint : f.points) {
                final double xDiff = loopPoint.x - tempCenter.x;
                final double yDiff = loopPoint.y - tempCenter.y;
                if (xDiff > 0 && yDiff > 0) {
                    pointSplit[0].add(loopPoint);
                } else if (xDiff < 0 && yDiff > 0) {
                    pointSplit[1].add(loopPoint);
                } else if (xDiff < 0 && yDiff < 0) {
                    pointSplit[2].add(loopPoint);
                } else {
                    pointSplit[3].add(loopPoint);
                }
            }

            if (debug) {
                System.out.println(
                        "split: " + f.points.size() + " into " + pointSplit[0].size() + ", " + pointSplit[1].size() +
                                ", " + pointSplit[2].size() + " and " + pointSplit[3].size());
            }

            // create new facilities
            final Facility f1 = makeFacility(pointSplit[0]);
            final Facility f2 = makeFacility(pointSplit[1]);
            final Facility f3 = makeFacility(pointSplit[2]);
            final Facility f4 = makeFacility(pointSplit[3]);

            return Arrays.asList(f1, f2, f3, f4);
        }

        private static Facility makeFacility(final Collection<Point> points) {
            final Point newCenter = Point.findCenter(points);
            final Facility newFacility = new Facility(newCenter);

            for (final Point loopPoint : points) {
                newFacility.addPoint(loopPoint);
            }

            return newFacility;
        }

        public final Point center;
        private double distance = 0.0;
        private double maxDistance = 0.0;
        private Set<Point> points = new HashSet<Point>();

        protected Facility(final Point center) {
            this.center = center;
        }

        public void addPoint(final Point p) {
            final double d = center.getDistance(p);
            if (d > maxDistance) {
                maxDistance = d;
            }
            distance += d;
            points.add(p);
        }

        public int numPoints() {
            return points.size();
        }

        public double getTotalDistance() {
            return distance;
        }

        @Override
        public String toString() {
            return "Facility{center: " + center + ", distance: " + distance + ", num-pts: " + points.size() + "}";
        }
    }

    protected static class Box {

        final double x1;
        final double y1;
        final double x2;
        final double y2;

        protected Box(final double x1, final double y1, final double x2, final double y2) {
            this.x1 = x1;
            this.y1 = y1;
            this.x2 = x2;
            this.y2 = y2;
        }

        public boolean contains(final Point p) {
            return (x1 <= p.x && y1 <= p.y && p.x <= x2 && p.y <= y2);
        }

        public Point midPoint() {
            return new Point((x1 + x2) / 2, (y1 + y2) / 2);
        }
    }

    protected static interface Position {
        int UNKNOWN = -2;
        int ROOT = -1;
        int TOP_LEFT = 0;
        int TOP_RIGHT = 1;
        int BOT_LEFT = 2;
        int BOT_RIGHT = 3;
    }
}
