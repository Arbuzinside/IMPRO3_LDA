package de.tub.dima.impro3.lda;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.math.DenseVector;
import org.apache.flink.ml.math.Matrix;


/**
 * Created by arbuzinside on 29.12.2015.
 */


public class LDA_Job {


    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String path = "src/main/resources/data.txt";
        DataSet<Tuple1<String>> rawLines = env.readCsvFile(path).lineDelimiter("\n").types(String.class);

        /**
         * TODO: set uniques ids in different way
         */
        //corpus.print();

        DataSet<Tuple2<Long, DenseVector>> corpus = rawLines.map(new DataParser()).setParallelism(1);

        /**
         * Default parameters:
         *
         *  S 1 4 16 64 256 1024 4096 16384     - mini batch size
            κ 0.9 0.9 0.8 0.7 0.6 0.5 0.5 0.5   - learning rate
            τ0 1024 1024 1024 1024 1024 1024 64 1 - early iterations
         *
         *
         */

        int numberOfTopics = 3;


        LDAModel ldaModel = new LDA().setK(numberOfTopics).run(corpus);

        // Output topics. Each is a distribution over words (matching word count vectors)
        System.out.println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize()
                + " words):");
        Matrix topics = ldaModel.getTopics();
        for (int topic = 0; topic < 3; topic++) {
            System.out.print("Topic " + topic + ":");
            for (int word = 0; word < ldaModel.vocabSize(); word++) {
                System.out.print(" " + topics.apply(word, topic));
            }
            System.out.println();
        }





    }


    /**
     * Map parser to split lines into vectors with unique ids
     */
    public static class DataParser implements MapFunction<Tuple1<String>, Tuple2<Long, DenseVector>> {

        private Long id;

        public DataParser() {
            this.id = (long) -1;
        }

        @Override
        public Tuple2<Long,DenseVector> map(Tuple1<String> s){
            id++;
            String[] sarray = s.f0.trim().split("\\s");
            double[] lineValues = new double[sarray.length];

            for(int i = 0; i < sarray.length; i++){
                lineValues[i] = Double.parseDouble(sarray[i]);
            }
            return new Tuple2<>(id, new DenseVector(lineValues));
        }
    }


}
