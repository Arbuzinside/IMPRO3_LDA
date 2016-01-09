package de.tub.dima.impro3.lda;

import org.apache.flink.ml.math.DenseVector;
import org.apache.flink.ml.math.Matrix;
import scala.Tuple2;

/**
 * Created by arbuzinside on 5.1.2016.
 */
public abstract class LDAModel {

    public abstract int k();

    public abstract int vocabSize();

    public abstract DenseVector docConcentration();

    public abstract double topicConcentration();

    public abstract double gammaShape();

    public abstract Matrix topicsMatrix();

    public abstract Tuple2<int[], double[]>[] describeTopics(int var1);

    public Tuple2<int[], double[]>[] describeTopics() {
        return this.describeTopics(this.vocabSize());
    }

    public LDAModel() {
    }

}
