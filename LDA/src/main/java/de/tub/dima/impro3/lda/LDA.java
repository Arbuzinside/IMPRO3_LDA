package de.tub.dima.impro3.lda;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.math.DenseVector;

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
        this.setAlpha(-1.0);
        this.setBeta(-1.0);
        this.setSeed(new Random().nextLong());
        this.ldaOptimizer = new OnlineLDAOptimizer();
    }







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
        this.beta = topicConcentration;
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

