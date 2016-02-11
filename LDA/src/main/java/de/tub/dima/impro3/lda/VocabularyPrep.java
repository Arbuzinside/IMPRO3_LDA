package de.tub.dima.impro3.lda;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
 
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
 
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by arbuzinside on 7.2.2016.
 */
public class VocabularyPrep {

    private final static Set<String> sWords = new HashSet<String>();


    private final static int wordLength = 3;

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        env.setParallelism(1);

        DataSource<String> input = env.readTextFile(Config.pathToWikiResults());

        // read input with df-cut
        DataSet<Tuple1<String>> terms = input.flatMap(new DataReader());

        DataSet<Tuple1<String>> stopWords = env.readCsvFile(Config.pathToStopWOrds())   .lineDelimiter("\n")
                .types(String.class);

       List<Tuple1<String>>   slist =   stopWords.collect();
     
        for (Tuple1<String> word: slist)
            sWords.add(word.f0);
        
        DataSet<Set<String>> broadcastSW = ExecutionEnvironment.getExecutionEnvironment().fromElements(sWords);

        // conditional counter per word per label
        DataSet<Tuple1<String>> filteredTerms = terms.flatMap(new FilterWords()) .withBroadcastSet(broadcastSW, "broadcastSW").distinct();


        DataSet<Tuple2<Long, Tuple1<String>>> vocabulary =DataSetUtils.zipWithUniqueId(filteredTerms).sortPartition(1, Order.ASCENDING);


        vocabulary.writeAsCsv(Config.pathToConditionals(), "\n", "\t", FileSystem.WriteMode.OVERWRITE);

        // word counts per label
       // DataSet<Tuple2<String, Long>> termLabelCounts = termCounts.flatMap(new LabelCount()).groupBy(0).aggregate(Aggregations.SUM,1); // IMPLEMENT ME

       // termLabelCounts.writeAsCsv(Config.pathToSums(), "\n", "\t", FileSystem.WriteMode.OVERWRITE);

        env.execute();
    }




    public static class FilterWords extends  RichFlatMapFunction<Tuple1<String>,Tuple1<String>> {

        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;




        private  HashSet<String> sWords;


        public void open(Configuration parameters) {


            List<HashSet<String>> bsw  =  getRuntimeContext().getBroadcastVariable("broadcastSW");
            sWords = bsw.get(0);
        }
        
        
        public void flatMap(Tuple1<String> word, Collector<Tuple1<String>> collector) throws Exception {

        	
        	 if (!sWords.contains(word.f0) && isAlpha(word.f0) && word.f0.toCharArray().length > wordLength)
        	 {
        		 

                 collector.collect(new Tuple1<String>(word.f0));
        		 
        }
        }
        


        public boolean isAlpha(String name) {
            char[] chars = name.toCharArray();

            for (char c : chars) {
                if(!Character.isLetter(c)) {
                    return false;
                }
            }
            return true;
        }


    }


    public static class DataReader implements FlatMapFunction<String, Tuple1<String>> {
        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
        public void flatMap(String line, Collector<Tuple1<String>> collector) throws Exception {

            String[] tokens = line.split("\t");
//            String label = tokens[0];
            String[] terms = tokens[1].split(",");

            for (String term : terms) {
                collector.collect(new Tuple1<String>(term));
            }
        }

    }

    public static class LabelCount implements FlatMapFunction<Tuple3<String, String, Long>, Tuple2<String, Long>> {

        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
        public void flatMap(Tuple3<String, String, Long> input, Collector<Tuple2<String, Long>> out){

            out.collect(new Tuple2<String, Long>(input.f0, input.f2));

        }
    }


}
