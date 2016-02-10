package de.tub.dima.impro3.lda;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.math.DenseVector;
import org.apache.flink.ml.math.Matrix;

import java.util.HashSet;
import java.util.Set;


/**
 * Created by arbuzinside on 29.12.2015.
 */


public class LDA_Job2 {


    final static Set<String> sWords = new HashSet<String>();

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();



        String path = Config.pathToCorpus();
        DataSet<String> rawLines = env.readTextFile(path);





        //corpus.print();

        DataSet<Tuple2<Long, DenseVector>> corpus = rawLines.map(new DataParser()).setParallelism(10);

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
                System.out.print(" " + topics.apply(topic, word));
            }
            System.out.println();
        }





    }


    /**
     * Map parser to split lines into vectors with unique ids
     */
    public static class DataParser implements MapFunction<String, Tuple2<Long, DenseVector>> {

        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private Long id;

        public DataParser() {
            this.id = (long) -1;
        }

        @Override
        public Tuple2<Long,DenseVector> map(String s){
            id++;
            String[] sarray = s.trim().split("\\,");
            
        	
//            Long key = Long.parseLong(sarray[0].replace("(", ""));
            String[] array  = sarray[1].split("DenseVector");
            sarray[1] =  array[1].replace("(", "");
            
            sarray[sarray.length-1] = sarray[sarray.length-1].replace(")", "");	 
            
            double[] lineValues = new double[sarray.length-1];

            for(int i = 1; i < sarray.length-1; i++){
                lineValues[i-1] = Double.parseDouble(sarray[i]);
            }
            return new Tuple2<>(id, new DenseVector(lineValues));
        }
    }




}
