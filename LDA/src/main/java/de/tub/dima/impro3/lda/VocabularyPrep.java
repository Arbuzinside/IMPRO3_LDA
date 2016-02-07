package de.tub.dima.impro3.lda;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by arbuzinside on 7.2.2016.
 */
public class VocabularyPrep {

    private final static Set<String> sWords = new HashSet<String>();


    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        env.setParallelism(1);

        DataSource<String> input = env.readTextFile(Config.pathToTrainingSet());

        // read input with df-cut
        DataSet<String> terms = input.flatMap(new DataReader());

        DataSet<String> stopWords = env.readTextFile(Config.pathToStopWOrds());

        String[] words = stopWords.toString().split("\\n");
        for (String word: words)
            sWords.add(word);

        // conditional counter per word per label
        DataSet<String> filteredTerms = terms.filter(new FilterWords()).distinct();


        DataSet<Tuple2<Long, String>> vocabulary = DataSetUtils.zipWithUniqueId(filteredTerms).sortPartition(1, Order.ASCENDING);


        vocabulary.writeAsCsv(Config.pathToConditionals(), "\n", "\t", FileSystem.WriteMode.OVERWRITE);

        // word counts per label
       // DataSet<Tuple2<String, Long>> termLabelCounts = termCounts.flatMap(new LabelCount()).groupBy(0).aggregate(Aggregations.SUM,1); // IMPLEMENT ME

       // termLabelCounts.writeAsCsv(Config.pathToSums(), "\n", "\t", FileSystem.WriteMode.OVERWRITE);

        env.execute();
    }




    public static class FilterWords implements FilterFunction<String> {

        @Override
        public boolean filter(String word) {


            if (!sWords.contains(word) && isAlpha(word) && word.toCharArray().length > 4)
                return true;
            else
                return false;

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


    public static class DataReader implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String line, Collector<String> collector) throws Exception {

            String[] tokens = line.split("\t");
            String label = tokens[0];
            String[] terms = tokens[1].split(",");

            for (String term : terms) {
                collector.collect(term);
            }
        }

    }

    public static class LabelCount implements FlatMapFunction<Tuple3<String, String, Long>, Tuple2<String, Long>> {

        @Override
        public void flatMap(Tuple3<String, String, Long> input, Collector<Tuple2<String, Long>> out){

            out.collect(new Tuple2<String, Long>(input.f0, input.f2));

        }
    }


}
