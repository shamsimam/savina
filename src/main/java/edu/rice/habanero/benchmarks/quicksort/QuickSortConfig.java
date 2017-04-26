package edu.rice.habanero.benchmarks.quicksort;

import edu.rice.habanero.benchmarks.BenchmarkRunner;
import edu.rice.habanero.benchmarks.PseudoRandom;

import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class QuickSortConfig {

    protected static int N = 1_000_000; // data size
    protected static long M = 1L << 60; // max value
    protected static long T = 2_048; // threshold to perform sort sequentially
    protected static long S = 1_024; // seed for random number generator
    protected static boolean debug = false;

    protected static void parseArgs(final String[] args) {
        int i = 0;
        while (i < args.length) {
            final String loopOptionKey = args[i];
            switch (loopOptionKey) {
                case "-n":
                    i += 1;
                    N = Integer.parseInt(args[i]);
                    break;
                case "-m":
                    i += 1;
                    M = Long.parseLong(args[i]);
                    break;
                case "-t":
                    i += 1;
                    T = Long.parseLong(args[i]);
                    break;
                case "-s":
                    i += 1;
                    S = Long.parseLong(args[i]);
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
        System.out.printf(BenchmarkRunner.argOutputFormat, "N (num values)", N);
        System.out.printf(BenchmarkRunner.argOutputFormat, "M (max value)", M);
        System.out.printf(BenchmarkRunner.argOutputFormat, "T (sequential cutoff)", T);
        System.out.printf(BenchmarkRunner.argOutputFormat, "S (seed)", S);
        System.out.printf(BenchmarkRunner.argOutputFormat, "debug", debug);
    }

    protected static List<Long> quicksortSeq(final List<Long> data) {

        final int dataLength = data.size();
        if (dataLength < 2) {
            return data;
        }

        final long pivot = data.get(dataLength / 2);

        final List<Long> leftUnsorted = filterLessThan(data, pivot);
        final List<Long> leftSorted = quicksortSeq(leftUnsorted);

        final List<Long> equalElements = filterEqualsTo(data, pivot);

        final List<Long> rightUnsorted = filterGreaterThan(data, pivot);
        final List<Long> rightSorted = quicksortSeq(rightUnsorted);

        final List<Long> sortedArray = new ArrayList<>(dataLength);
        sortedArray.addAll(leftSorted);
        sortedArray.addAll(equalElements);
        sortedArray.addAll(rightSorted);

        return sortedArray;
    }

    protected static List<Long> filterLessThan(final List<Long> data, final long pivot) {
        final int dataLength = data.size();
        final List<Long> result = new ArrayList<>(dataLength);

        for (final Long loopItem : data) {
            if (loopItem < pivot) {
                result.add(loopItem);
            }
        }

        return result;
    }

    protected static List<Long> filterEqualsTo(final List<Long> data, final long pivot) {
        final int dataLength = data.size();
        final List<Long> result = new ArrayList<>(dataLength);

        for (final Long loopItem : data) {
            if (loopItem == pivot) {
                result.add(loopItem);
            }
        }

        return result;
    }

    protected static List<Long> filterBetween(final List<Long> data, final long leftPivot, final long rightPivot) {
        final int dataLength = data.size();
        final List<Long> result = new ArrayList<>(dataLength);

        for (final Long loopItem : data) {
            if ((loopItem >= leftPivot) && (loopItem <= rightPivot)) {
                result.add(loopItem);
            }
        }

        return result;
    }

    protected static List<Long> filterGreaterThan(final List<Long> data, final long pivot) {
        final int dataLength = data.size();
        final List<Long> result = new ArrayList<>(dataLength);

        for (final Long loopItem : data) {
            if (loopItem > pivot) {
                result.add(loopItem);
            }
        }

        return result;
    }

    protected static void checkSorted(final List<Long> data) {
        final int length = data.size();
        if (length != N) {
            throw new RuntimeException("result is not correct length, expected: " + N + ", found: " + length);
        }

        long loopValue = data.get(0);
        int nextIndex = 1;
        while (nextIndex < length) {
            final long temp = data.get(nextIndex);
            if (temp < loopValue) {
                throw new RuntimeException("result is not sorted, cur index: " + nextIndex + ", cur value: " + temp + ", prev value: " + loopValue);
            }
            loopValue = temp;
            nextIndex += 1;

        }
    }

    protected static List<Long> randomlyInitArray() {

        final List<Long> result = new ArrayList<>(N);

        final PseudoRandom random = new PseudoRandom(S);
        for (int i = 0; i < N; i++) {
            result.add(Math.abs(random.nextLong() % M));
        }

        return result;
    }
}
