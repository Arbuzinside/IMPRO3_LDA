package de.tub.dima.impro3.lda;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.math.DenseVector;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.Pair;
import org.apache.flink.util.Collector;


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

        DataSet<Tuple2<Long, DenseVector>> corpus = rawLines.map(new DataParser()).setParallelism(1);


        corpus.print();





    }


    /**
     *  Map parser to split lines into vectors with unique ids
     */
    public static class DataParser implements MapFunction<Tuple1<String>, Tuple2<Long, DenseVector>> {

        private Long id;

        public DataParser(){
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
            return new Tuple2<Long, DenseVector>(id, new DenseVector(lineValues));
        }
    }




}
