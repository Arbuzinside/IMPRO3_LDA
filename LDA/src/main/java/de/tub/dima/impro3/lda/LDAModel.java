package de.tub.dima.impro3.lda;

import org.apache.flink.ml.math.DenseMatrix;

/**
 * Created by arbuzinside on 5.1.2016.
 */
public class LDAModel {



    private DenseMatrix topics;
    private double docConcentration;
    private double topicConcentration;
    private double gammaShape;

    private long vocabSize;




    public LDAModel(DenseMatrix topics, double docConcentration, double topicConcentration, double gammaShape) {
        this.setTopics(topics);
        this.setDocConcentration(docConcentration);
        this.setTopicConcentration(topicConcentration);
        this.setGammaShape(gammaShape);
    }

    public DenseMatrix getTopics() {
        return topics;
    }

    public void setTopics(DenseMatrix topics) {
        this.topics = topics;
    }

    public long vocabSize(){

        return vocabSize;
    }


    public double getDocConcentration() {
        return docConcentration;
    }

    public void setDocConcentration(double docConcentration) {
        this.docConcentration = docConcentration;
    }

    public double getTopicConcentration() {
        return topicConcentration;
    }

    public void setTopicConcentration(double topicConcentration) {
        this.topicConcentration = topicConcentration;
    }

    public double getGammaShape() {
        return gammaShape;
    }

    public void setGammaShape(double gammaShape) {
        this.gammaShape = gammaShape;
    }
}
