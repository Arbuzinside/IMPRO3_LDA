package de.tub.dima.impro3.lda;

/**
 * Created by arbuzinside on 5.1.2016.
 */

import breeze.stats.distributions.Gamma;
import breeze.stats.distributions.RandBasis;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.random.MersenneTwister;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.math.DenseMatrix;
import org.apache.flink.ml.math.DenseVector;
import org.apache.flink.util.Collector;
import scala.reflect.ClassTag;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;


public class OnlineLDAOptimizer {


    public final static double MEAN_CHANGE_THRESHOLD = 1e-3;
    public final static int NUM_ITERATIONS = 20;

    private static Random randomGenerator;

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


    private int iteration;

    /**
     * Learning rate: exponential decay rate---should be between
     * (0.5, 1.0] to guarantee asymptotic convergence.
     * Default: 0.51, based on the original Online LDA paper.
     */
    private double gammaShape = 100.0d;
    private double kappa = 0.51d;
    /**
     * A (positive) learning parameter that downweights early iterations. Larger values make early
     * iterations count less.
     * Default: 1024, following the original Online LDA paper.
     */
    private  double tau0 = 1024.0d;
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
    private double miniBatchFraction = 0.05;
    private int batchCount;
    //document concentration
    private  double alpha;
    //topic concentration
    private  double eta;

    private double rhot;


    //TODO: implement sampling technique

    private  List<Tuple2<Long, DenseVector>> corpus;
    int batchSize;
    private List<Tuple2<Long, DenseVector>> sublist;

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
        this.lambda = getGammaMatrix(K, vocabSize);
        //posterior over topics -beta is parameterized by lambda
    }

    public OnlineLDAOptimizer(){



        }






    OnlineLDAOptimizer initialize(DataSet<Tuple2<Long, DenseVector>> docs, LDA lda){

        try {
            this.K = lda.getK();
            this.corpusSize = (int) docs.count();

            //TODO: vocab size in unclear
            this.vocabSize = docs.collect().get(0).f1.size();

            this.alpha = 1.0 /lda.getK();
            this.docs = docs;


            this.batchSize = (int) Math.round(docs.count() * miniBatchFraction);
          //  this.corpus = docs.collect();

            // Initialize the variational distribution q(beta|lambda)
            randomGenerator = new Random(lda.getSeed());
            this.lambda = getGammaMatrix(K, vocabSize);
            this.iteration = 0;
            this.eta = 1.0 / lda.getK();


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

        Double[] temp = (Double[]) gammaRandomGenerator.sample(rows * cols).toArray(tag);

        double[] d = ArrayUtils.toPrimitive(temp);

        return new DenseMatrix(rows, cols, d);
    }


    /**
     * TODO: change into many batches (streaming)
     * @return
     */

    OnlineLDAOptimizer next(){



        DataSet<Tuple2<Long, DenseVector>> batch =  DataSetUtils.sample(docs, true, miniBatchFraction, randomGenerator.nextLong());

        try {

            if (batch.collect().isEmpty())
                return  this;

            return this.submitMiniBatch(batch);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }



    LDAModel getLDAModel(){


        DenseMatrix res = lambda;

        return new LDAModel(res, this.alpha, this.eta, this.gammaShape, this.vocabSize);


    }


    OnlineLDAOptimizer submitMiniBatch(DataSet<Tuple2<Long, DenseVector>> batch) throws Exception {


        this.iteration += 1;
        int vocabSize = this.vocabSize;





        DenseMatrix eLogTheta =   LDAUtils.dirichletExpectation(this.lambda);
        DenseMatrix expELogBeta = LDAUtils.exp(eLogTheta);
        DenseMatrix expELogBetaTr = LDAUtils.transpose(expELogBeta);
        DataSet<DenseMatrix> broadcast = ExecutionEnvironment.getExecutionEnvironment().fromElements(expELogBetaTr);

        double alpha = this.alpha;
        //variational parameter over documents x topics
        //parameters over documents x topics
        double gammaShape = this.gammaShape;

        //filter empty docs out
        DataSet<Tuple2<Long, DenseVector>> filteredDocs = batch.filter(new docFilter());

        //provide local index to the docs


        DataSet<Tuple2<Long, Tuple2<Long, DenseVector>>> localBatchIndex = DataSetUtils.zipWithUniqueId(filteredDocs);


        //prepare common parameters
        Configuration params = new Configuration();
        params.setInteger("k", K);
        params.setInteger("vocabSize", vocabSize);
        params.setDouble("alpha", alpha);
        params.setDouble("gammaShape", gammaShape);


        DataSet<Tuple2<HashMap<Long, DenseVector>, DenseMatrix>> expectation = localBatchIndex.mapPartition(new batchMapper())
                .withParameters(params).withBroadcastSet(broadcast, "broadcast");





        DataSet<DenseMatrix> docStats = expectation.map(new MapFunction<Tuple2<HashMap<Long, DenseVector>, DenseMatrix>, DenseMatrix>() {
                                                            @Override
                                                            public DenseMatrix map(Tuple2<HashMap<Long, DenseVector>, DenseMatrix> hashMapDenseMatrixTuple2) throws Exception {
                                                                return hashMapDenseMatrixTuple2.f1;
                                                            }
                                                        });




        DataSet<DenseMatrix> statsSum = docStats.reduce(new statReducer());





        try {


            DenseMatrix statsRes = statsSum.collect().get(0);
            DenseVector batchVector = LDAUtils.product(new DenseVector(statsRes.data()), new DenseVector(expELogBeta.data()));
            DenseMatrix batchResult = new DenseMatrix(statsRes.numRows(), expELogBeta.numCols(), batchVector.data());





            updateLambda(batchResult, Math.ceil(miniBatchFraction * (double) corpusSize));

        }
        catch (Exception ex){

            ex.printStackTrace();
        }




        return this;
    }



    private void updateLambda(DenseMatrix stat, double batchSize){

        double weight = countRho();



        DenseMatrix a = LDAUtils.product(lambda, 1 - weight);
        DenseMatrix b = LDAUtils.addToMatrix(LDAUtils.product(stat,(double)corpusSize/batchSize), this.eta);

        b = LDAUtils.product(b, weight);
        this.lambda = LDAUtils.sum(a,b);



    }


    private double countRho(){


        return Math.pow(tau0 + this.iteration, -this.kappa);
    }




    //TODO: optimization not sure if needed
    private void updateAlpha(){



    }








    // ReduceFunction that sums Integers
    public static class statReducer implements ReduceFunction<DenseMatrix> {


        @Override
        public DenseMatrix reduce(DenseMatrix m1, DenseMatrix m2) throws Exception {
            return LDAUtils.sum(m1,m2);
        }
    }





    public static class batchMapper extends RichMapPartitionFunction<Tuple2<Long, Tuple2<Long, DenseVector>>, Tuple2<HashMap<Long, DenseVector>, DenseMatrix>> {


        private int k;
        int vocabSize;
        DenseMatrix expELogBeta;
        double alpha;
        double gammaShape;



        public void open(Configuration parameters) {
            this.k = parameters.getInteger("k", 10);
            this.vocabSize = parameters.getInteger("vocabSize", 10000);
            this.alpha = parameters.getDouble("alpha", 1 / 10d);
            this.gammaShape = parameters.getDouble("gammaShape", 100d);


            List<DenseMatrix> beta = getRuntimeContext().getBroadcastVariable("broadcast");
            this.expELogBeta = beta.get(0);
        }



        @Override
        public void mapPartition(Iterable<Tuple2<Long, Tuple2<Long, DenseVector>>> docs, Collector<Tuple2<HashMap<Long, DenseVector>, DenseMatrix>> collector) throws Exception {

            DenseMatrix stats = DenseMatrix.zeros(k, vocabSize);

            HashMap<Long, DenseVector> gamma = new HashMap<>();

            for (Tuple2<Long, Tuple2<Long,DenseVector>> doc: docs) {

                Tuple2<Long, DenseVector> currentDoc = doc.f1;
                DenseVector wordCounts = currentDoc.f1;

                Tuple2<DenseVector, DenseMatrix> result = variationalTopicInference(wordCounts, expELogBeta, alpha, gammaShape, k);

                assert result != null;
                stats = LDAUtils.sum(stats, result.f1);

                gamma.put(doc.f1.f0, result.f0);
            }


            collector.collect(new Tuple2<>(gamma, stats));

        }



    }

    public static class docFilter implements FilterFunction<Tuple2<Long, DenseVector>> {


        @Override
        public boolean filter(Tuple2<Long, DenseVector> doc) throws Exception {
            return doc.f1.magnitude() != 0.0;
        }

    }


    public static Tuple2<DenseVector, DenseMatrix> variationalTopicInference(DenseVector termCounts, DenseMatrix expELogBeta, double alpha,
                                                                             double gammaShape, Integer K){




        List<Integer> ids = new ArrayList<>(termCounts.size());

        double[] cts = termCounts.data();


        for(int i = 0; i< termCounts.size(); i++){
            ids.add(i, i);
        }






        if(!ids.isEmpty() && cts != null) {


            RandBasis randBasis = new RandBasis(new MersenneTwister(randomGenerator.nextLong()));
            ClassTag<Double> tag = scala.reflect.ClassTag$.MODULE$.apply(Double.class);
            double[] d = ArrayUtils.toPrimitive((Double[]) new Gamma(gammaShape, 1.0 / gammaShape, randBasis).samplesVector(K, tag).data());

            DenseVector gammaD =  new DenseVector(d);


            DenseVector expELogThetaD = LDAUtils.exp(LDAUtils.dirichletExpectation(gammaD));
            DenseMatrix expELogBetaD = LDAUtils.extractRows(expELogBeta, ids);
            DenseVector phiNorm = LDAUtils.dot(expELogBetaD, expELogThetaD);
            phiNorm = LDAUtils.addToVector(phiNorm, 1.0E-100D);

            DenseVector lastGamma;


            // Iterate between gamma and phi until convergence

            int iterations = 0;

            while(iterations < NUM_ITERATIONS) {

                lastGamma = gammaD.copy();

                DenseVector temp = LDAUtils.divideVectors(new DenseVector(cts), phiNorm);
                DenseVector v1 = LDAUtils.dot(LDAUtils.transpose(expELogBetaD), temp);
                DenseVector v2 = LDAUtils.product(expELogThetaD, v1);

                gammaD = LDAUtils.addToVector(v2, alpha);

                expELogThetaD = LDAUtils.exp(LDAUtils.dirichletExpectation(gammaD));


                phiNorm = LDAUtils.dot(expELogBetaD, expELogThetaD);
                phiNorm = LDAUtils.addToVector(phiNorm, 1.0E-100D);

                if (LDAUtils.closeTo(gammaD, lastGamma, MEAN_CHANGE_THRESHOLD))
                    break;

                iterations++;
            }

            DenseVector temp = LDAUtils.divideVectors(new DenseVector(cts),  phiNorm);
            DenseMatrix sstatsD =  LDAUtils.outer(expELogThetaD, temp);

            return new Tuple2<>(gammaD, sstatsD);
        }




        return null;






    }

}