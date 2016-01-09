package de.tub.dima.impro3.lda;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.math.DenseVector;
import org.apache.flink.ml.math.Vector;

import java.util.Random;

/**
 * Created by arbuzinside on 4.1.2016.
 */
public class LDA {


    private Integer K;
    private Integer maxIterations;
    private Double alpha;
    private Double beta;
    private OnlineLDAOptimizer ldaOptimizer;
    private long seed;




    public LDA() {

        this.setK(10);
        this.setMaxIterations(20);
        this.setAlpha(1d/K);
        this.setBeta(1d/K);
        this.setSeed(new Random().nextLong());
        this.ldaOptimizer = new OnlineLDAOptimizer();
    }

    public LDA(int k, int maxIterations, double alpha, double beta) {

        this();
        this.setK(k);
        this.setBeta(beta);
        this.setAlpha(alpha);
        this.setMaxIterations(maxIterations);

    }


    public void computePTopic() {


    }


    /**
     * Learn an LDA model using the given dataset.
     *
     * @param documents RDD of documents, which are term (word) count vectors paired with IDs.
     *                  The term count vectors are "bags of words" with a fixed-size vocabulary
     *                  (where the vocabulary size is the length of the vector).
     *                  Document IDs must be unique and >= 0.
     * @return Inferred LDA model
     */


    /*
    def run(documents: RDD[(Long, Vector)]): LDAModel = {
        val state = ldaOptimizer.initialize(documents, this)
        var iter = 0
        val iterationTimes = Array.fill[Double](maxIterations)(0)
        while (iter < maxIterations) {
            val start = System.nanoTime()
            state.next()
            val elapsedSeconds = (System.nanoTime() - start) / 1e9
            iterationTimes(iter) = elapsedSeconds
            iter += 1
        }
        state.getLDAModel(iterationTimes)
    }
        */

    public LDAModel run(DataSet<Tuple2<Long, DenseVector>> corpus){

        OnlineLDAOptimizer state = this.ldaOptimizer.initialize(corpus, this);

        int iter = 0;

        while(iter < maxIterations){

            state.next();
            iter++;
        }

         return state.getLDAModel();

    }


    public Integer getK() {
        return K;
    }

    public LDA setK(Integer k) {

        if (k > 0)
            this.K = k;
        else throw new IndexOutOfBoundsException("k should be bigger than zero");
        return this;
    }

    public Integer getMaxIterations() {
        return maxIterations;
    }

    public void setMaxIterations(Integer maxIterations) {
        this.maxIterations = maxIterations;
    }




    public Double getAlpha() {
        return this.alpha;
    }

    public LDA setAlpha(double alpha) {
        this.alpha = alpha;
        return this;
    }



    public Double getBeta() {
        return beta;
    }


    public LDA setBeta(Double topicConcentration) {
        this.beta= topicConcentration;
        return this;
    }


    public long getSeed() {
        return seed;
    }

    public LDA setSeed(long seed) {
        this.seed = seed;
        return this;
    }
}
