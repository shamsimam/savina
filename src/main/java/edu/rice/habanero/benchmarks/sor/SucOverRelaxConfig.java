package edu.rice.habanero.benchmarks.sor;

import edu.rice.habanero.benchmarks.BenchmarkRunner;
import edu.rice.habanero.benchmarks.PseudoRandom;

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class SucOverRelaxConfig {

    // ORIGINAL val REF_VAL = Array[Double](0.498574406322512, 1.1234778980135105, 1.9954895063582696)
    protected static final double[] REF_VAL = {
            0.000003189420084871275,
            0.001846644602759566,
            0.0032099996270638005,
            0.0050869220175413146,
            0.008496328291240363,
            0.016479973604143234,
            0.026575660248076397,
            1.026575660248076397,
            2.026575660248076397,
            3.026575660248076397};
    protected static final int[] DATA_SIZES = {20, 80, 100, 120, 150, 200, 250, 300, 350, 400};
    protected static final int JACOBI_NUM_ITER = 100;
    protected static final double OMEGA = 1.25;
    protected static final long RANDOM_SEED = 10101010;

    protected static int N = 0;
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
                case "-debug":
                case "-verbose":
                    debug = true;
                    break;
            }
            i += 1;
        }
    }

    protected static void printArgs() {
        System.out.printf(BenchmarkRunner.argOutputFormat, "N (input size)", N);
        System.out.printf(BenchmarkRunner.argOutputFormat, "Data size", DATA_SIZES[N]);
        System.out.printf(BenchmarkRunner.argOutputFormat, "debug", debug);
    }

    protected static void performComputation(final double theta) {
        final double sint = Math.sin(theta);
        final double res = sint * sint;
        //defeat dead code elimination
        if (res <= 0) {
            throw new IllegalStateException("Benchmark exited with unrealistic res value " + res);
        }
    }

    protected static PseudoRandom R = null;
    protected static double[][] A = null;

    protected static void initialize() {
        R = resetRandomGenerator();
        final int dataSize = SucOverRelaxConfig.DATA_SIZES[SucOverRelaxConfig.N];
        A = randomMatrix(dataSize, dataSize);
    }

    protected static PseudoRandom resetRandomGenerator() {
        return new PseudoRandom(RANDOM_SEED);
    }

    protected static double[][] randomMatrix(final int M, final int N) {
        final double[][] A = new double[M][N];

        for (int i = 0; i < M; i++) {
            for (int j = 0; j < N; j++) {
                A[i][j] = R.nextDouble() * 1e-6;
            }
        }

        return A;
    }

    protected static void jgfValidate(final double gTotal, final int size) {
        final double dev = Math.abs(gTotal - REF_VAL[size]);
        if (dev > 1.0e-12) {
            System.out.println("Validation failed");
            System.out.println("Gtotal = " + gTotal + "  " + REF_VAL[size] + "  " + dev + "  " + size);
        } else {
            System.out.println("Validation OK!");
        }
    }
}
