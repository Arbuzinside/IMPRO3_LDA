package de.tub.dima.impro3.lda;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.math.DenseVector;
import org.apache.flink.ml.math.Vector;

import java.util.Random;

/**
 * Created by arbuzinside on 4.1.2016.
 */
public class LDA {


    private Integer k;
    private Integer maxIterations;
    private Vector docConcentration;
    private Double topicConcentration;
    private Long seed;
    private Integer checkpointInterval;
    private LDAOptimizer ldaOptimizer;


    public LDA(){
        this.setK(10);
        this.setMaxIterations(20);
        this.setAlpha(new DenseVector(new double[]{-1}));
        this.setBeta(- 1.0);
        this.setSeed(new Random().nextLong());
        this.setCheckpointInterval(10);
        this.ldaOptimizer = new LDAOptimizer();
    }

    public LDA(int k){

        this();
        this.setK(k);

    }




    /**
     * Learn an LDA model using the given dataset.
     *
     * @param documents  RDD of documents, which are term (word) count vectors paired with IDs.
     *                   The term count vectors are "bags of words" with a fixed-size vocabulary
     *                   (where the vocabulary size is the length of the vector).
     *                   Document IDs must be unique and >= 0.
     * @return  Inferred LDA model
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


    public LDAModel run(Tuple2<Long, DenseVector> corpus){

        int iter = 0;

        while (iter < maxIterations){
            long start = System.nanoTime();



            iter += 1;
        }



    }












    /**
     * Incupsulations
     *
     */


    public double getDocConcentration(){
        if (this.docConcentration.size() == 1)
            return this.docConcentration.apply(0);
        else throw new IndexOutOfBoundsException("Alpha is Asymmetric");
    }

    public Integer getK() {
        return k;
    }

    public void setK(Integer k) {

        if (k > 0)
            this.k = k;
        else throw new IndexOutOfBoundsException("k should be bigger than zero");
    }

    public Integer getMaxIterations() {
        return maxIterations;
    }

    public void setMaxIterations(Integer maxIterations) {
        this.maxIterations = maxIterations;
    }

    public Vector getAsymmetricAlpha() {
        return docConcentration;
    }


    /**
     * Concentration parameter (commonly named "alpha") for the prior placed on documents'
     * distributions over topics ("theta").
     *
     * This is the parameter to a Dirichlet distribution, where larger values mean more smoothing
     * (more regularization).
     *
     * If set to a singleton vector Vector(-1), then docConcentration is set automatically. If set to
     * singleton vector Vector(t) where t != -1, then t is replicated to a vector of length k during
     * [[LDAOptimizer.initialize()]]. Otherwise, the [[docConcentration]] vector must be length k.
     * (default = Vector(-1) = automatic)
     *
     * Optimizer-specific parameter settings:
     *  - EM
     *     - Currently only supports symmetric distributions, so all values in the vector should be
     *       the same.
     *     - Values should be > 1.0
     *     - default = uniformly (50 / k) + 1, where 50/k is common in LDA libraries and +1 follows
     *       from Asuncion et al. (2009), who recommend a +1 adjustment for EM.
     *  - Online
     *     - Values should be >= 0
     *     - default = uniformly (1.0 / k), following the implementation from
     *       [[https://github.com/Blei-Lab/onlineldavb]].
     */
    public void setAlpha(Vector docConcentration) {
        this.docConcentration = docConcentration;
    }

    public void setAlpha(double alpha){
        this.docConcentration = new DenseVector(new double[]{alpha});
    }



    /**
     * Concentration parameter (commonly named "beta" or "eta") for the prior placed on topics'
     * distributions over terms.
     *
     * This is the parameter to a symmetric Dirichlet distribution.
     *
     * Note: The topics' distributions over terms are called "beta" in the original LDA paper
     * by Blei et al., but are called "phi" in many later papers such as Asuncion et al., 2009.
     *
     * If set to -1, then topicConcentration is set automatically.
     *  (default = -1 = automatic)
     *
     * Optimizer-specific parameter settings:
     *  - EM
     *     - Value should be > 1.0
     *     - default = 0.1 + 1, where 0.1 gives a small amount of smoothing and +1 follows
     *       Asuncion et al. (2009), who recommend a +1 adjustment for EM.
     *  - Online
     *     - Value should be >= 0
     *     - default = (1.0 / k), following the implementation from
     *       [[https://github.com/Blei-Lab/onlineldavb]].
     */



    public Double getBeta() {
        return topicConcentration;
    }

    /**
     * Maximum number of iterations for learning.
     * (default = 20)
     */
    public void setBeta(Double topicConcentration) {
        this.topicConcentration = topicConcentration;
    }

    public Long getSeed() {
        return seed;
    }

    public void setSeed(Long seed) {
        this.seed = seed;
    }


    /**
     * Period (in iterations) between checkpoints (default = 10). Checkpointing helps with recovery
     * (when nodes fail). It also helps with eliminating temporary shuffle files on disk, which can be
     * important when LDA is run for many iterations. If the checkpoint directory is not set in
     * [[org.apache.spark.SparkContext]], this setting is ignored.
     *
     * @see [[org.apache.spark.SparkContext
     */

    public Integer getCheckpointInterval() {
        return checkpointInterval;
    }

    public void setCheckpointInterval(Integer checkpointInterval) {
        this.checkpointInterval = checkpointInterval;
    }
}
