package de.tub.dima.impro3.lda;

/**
 * Created by arbuzinside on 5.1.2016.
 */

import breeze.stats.distributions.Gamma;
import breeze.stats.distributions.RandBasis;
import org.apache.commons.math3.random.MersenneTwister;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.math.DenseMatrix;
import org.apache.flink.ml.math.DenseVector;
import org.apache.flink.util.Collector;
import scala.reflect.ClassTag;

import java.util.List;
import java.util.Random;


public class OnlineLDAOptimizer {


    public final static double MEAN_CHANGE_THRESHOLD = 1e-5;
    public final static int NUM_ITERATIONS = 200;

    private Random randomGenerator;

    /**
     * Common variables
     */
    //K - number of topics
    private int K;
    //size of vocabulary
    private  int vocabSize;
    //size of the corpus
    private int corpusSize;

    private DataSet<Tuple2<Long, DenseVector>> docs;

    private LDAModel model;
    private int iteration;

    /**
     * Learning rate: exponential decay rate---should be between
     * (0.5, 1.0] to guarantee asymptotic convergence.
     * Default: 0.51, based on the original Online LDA paper.
     */
    private double gammaShape;
    private double kappa;
    /**
     * A (positive) learning parameter that downweights early iterations. Larger values make early
     * iterations count less.
     * Default: 1024, following the original Online LDA paper.
     */
    private  double tau0;
    /**
     * Mini-batch fraction in (0, 1], which sets the fraction of document sampled and used in
     * each iteration.
     *
     * Note that this should be adjusted in synch with [[LDA.setMaxIterations()]]
     * so the entire corpus is used.  Specifically, set both so that
     * maxIterations * miniBatchFraction >= 1.
     *
     * Default: 0.05, i.e., 5% of total documents.
     */
    private int batchCount;
    //document concentration
    private  double alpha;
    //topic concentration
    private  double eta;

    private double rhot;

    // the variational distribution q(beta|lambda)
    private DenseMatrix lambda;
    private DenseMatrix eLogBeta;
    private DenseMatrix expELogBeta;
    private DenseMatrix stats;
    private DenseMatrix gamma;

    /**
     * For a vector theta ~ Dir(alpha), computes E[log(theta)] given alpha.
     *
     * @param vocabSize  - vocabulary length
     * @param K          - number of topics
     * @param corpusSize - total number of documents in the population.
     * @param alpha      - hyperparameter for prior on weight vectors theta
     * @param eta        - hyperparameter for prior on topics beta
     * @param tau        - controls early iterations
     * @param kappa      -  learning rate: exponential decay rate, should be within (0.5, 1.0].
     */
    public OnlineLDAOptimizer(int vocabSize, int K, int corpusSize, double alpha,
                              double eta, double tau, double kappa) {
        this.K = K;
        this.corpusSize = corpusSize;
        this.vocabSize = vocabSize;
        this.alpha = alpha;


        this.eta = eta;
        this.tau0 = tau + 1;
        this.kappa = kappa;
        this.batchCount = 0;
        //initialize the variational distribution q(beta|lambda)
        this.lambda = getGammaMatrix(vocabSize, K);
        //posterior over topics -beta is parameterized by lambda
    }

    public OnlineLDAOptimizer(){



        }






    OnlineLDAOptimizer initialize(DataSet<Tuple2<Long, DenseVector>> docs, LDA lda){

        try {
            this.K = lda.getK();
            this.corpusSize = (int) docs.count();

            //TODO: vocab size in unclear
            this.vocabSize = 10000;

            this.alpha = lda.getAlpha();
            this.docs = docs;

            // Initialize the variational distribution q(beta|lambda)
            this.lambda = getGammaMatrix(vocabSize, K);
            this.iteration = 0;
            this.randomGenerator = new Random(lda.getSeed());

        } catch (Exception ex){
            ex.printStackTrace();
        }

        return this;

    }

    //TODO: gammaShape
    private DenseMatrix getGammaMatrix(int rows, int cols){

        RandBasis randBasis = new RandBasis(new MersenneTwister(randomGenerator.nextLong()));
        Gamma gammaRandomGenerator = new Gamma(gammaShape, 1.0D/gammaShape, randBasis);


        ClassTag<Double> tag = scala.reflect.ClassTag$.MODULE$.apply(Double.class);

        double[] temp = (double[]) gammaRandomGenerator.sample(rows * cols).toArray(tag);

        return new DenseMatrix(rows,cols,temp);
    }


    /**
     * TODO: change into many batches (streaming)
     * @return
     */

    OnlineLDAOptimizer next(){


        DataSet<Tuple2<Long, DenseVector>> batch = docs;

        return this.submitMiniBatch(batch);
    }

    LDAModel getLDAModel(){

        return null;
    }


    OnlineLDAOptimizer submitMiniBatch(DataSet<Tuple2<Long, DenseVector>> batch){

        this.iteration += 1;
        int vocabSize = this.vocabSize;


        DenseMatrix eLogTheta =   LDAUtils.dirichletExpectation(this.gamma);
        DenseMatrix expElogbeta = LDAUtils.exp(eLogTheta);

        double alpha = this.alpha;
        //variational parameter over documents x topics
        //parameters over documents x topics
        double gammaShape = this.gammaShape;



        DataSet<Tuple2<DenseMatrix, List<DenseVector>>> stats = batch.mapPartition(new batchMapper());



        return this;
    }

    public static class batchMapper extends RichMapPartitionFunction<Tuple2<Long, DenseVector>, Tuple2<DenseMatrix, List<DenseVector>>> {



        @Override
        public void mapPartition(Iterable<Tuple2<Long, DenseVector>> iterable, Collector<Tuple2<DenseMatrix, List<DenseVector>>> collector) throws Exception {

        }
    }



}