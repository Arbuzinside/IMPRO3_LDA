package de.tub.dima.impro3.lda;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.ml.math.DenseVector;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by arbuzinside on 7.2.2016.
 */
public class DocToVec {



    private final static Set<String> sWords = new HashSet<String>();

    private static HashMap<String, Long> vocab;


    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();



        DataSet<String> stopWords = env.readTextFile(Config.pathToStopWOrds());

        String[] words = stopWords.toString().split("\\n");
        for (String word: words)
            sWords.add(word);


        DataSet<Tuple2<Long, String>> vocabulaty = env.readCsvFile(Config.pathToConditionals())
                .fieldDelimiter("\t")
                .lineDelimiter("\n")
                .types(Long.class, String.class);



         List vocList = vocabulaty.project(1, 0).collect();

        ArrayList<Tuple2<String, Long>> v = new ArrayList<>(vocList);


        vocab = listToMap(v);



        DataSet<HashMap<String, Long>> broadcastV = ExecutionEnvironment.getExecutionEnvironment().fromElements(vocab);
        DataSet<Set<String>> broadcastSW = ExecutionEnvironment.getExecutionEnvironment().fromElements(sWords);




        DataSet<Tuple2<String, String>> ldasrc = env.readCsvFile(Config.pathToTestSet())
                .fieldDelimiter("\t")
                .ignoreInvalidLines()
                .types(String.class, String.class)
                .filter(new LdaSrcFileFilter())
                .map(new LdaSrcFileMapper());


        DataSet<Tuple2<Long, Tuple2<String, String>>> docs =
                DataSetUtils.zipWithUniqueId(ldasrc);


        DataSet<Tuple2<Long, DenseVector>> corpus = docs.flatMap(new docToVector())
                .withBroadcastSet(broadcastV, "broadcastV")
                .withBroadcastSet(broadcastSW, "broadcastSW");





        corpus.writeAsText(Config.pathToCorpus(), FileSystem.WriteMode.OVERWRITE);



          env.execute();
    }




    public static class docToVector extends RichFlatMapFunction<Tuple2<Long, Tuple2<String, String>>, Tuple2<Long, DenseVector>> {


        private  HashSet<String> sWords;

        private  Map<String, Long> vocab;





        public void open(Configuration parameters) {

            List<HashMap<String, Long>> bv  =  getRuntimeContext().getBroadcastVariable("broadcastV");
            this.vocab = bv.get(0);


            List<HashSet<String>> bsw  =  getRuntimeContext().getBroadcastVariable("broadcastSW");
            sWords = bsw.get(0);
        }



        @Override
        public void flatMap(Tuple2<Long, Tuple2<String, String>> input, Collector<Tuple2<Long, DenseVector>> collector) throws Exception {


            HashMap<String, Integer> docWordCount = new HashMap<>();


            double[] size = new double[vocab.size()];
            Arrays.fill(size, 0.0);
            DenseVector output = new DenseVector(size);

            String[] words = input.f1.f1.toLowerCase().split("\\w+");


            for(String word: words){
                if(word.length() > 4 && !sWords.contains(word)) {
                    if (!docWordCount.containsKey(word))
                        docWordCount.put(word, 1);
                    else {
                        int count = docWordCount.get(word);
                        docWordCount.put(word, count + 1);
                    }
                }
            }




            for(String word: docWordCount.keySet()){
                long index = vocab.get(word);
                double count = (double) docWordCount.get(word);
                output.update((int) index, count);
            }






            collector.collect(new Tuple2<>(input.f0, output));

        }
    }



    public static HashMap listToMap(ArrayList<Tuple2<String, Long>> vocab){


        HashMap<String, Long> words = new HashMap<>();


        for(Tuple2<String, Long> tuple: vocab){

            words.put(tuple.f0, tuple.f1);

        }



        return words;
    }



    @SuppressWarnings("serial")
    private static class LdaSrcFileFilter implements FilterFunction<Tuple2<String, String>> {
        @Override
        public boolean filter(Tuple2<String, String> tuple) throws Exception {
            return !tuple.f1.isEmpty() && tuple.f1.length() > 0;
        }
    }

    @SuppressWarnings("serial")
    // INPUT : "document_id, content"
    // OUTPUT: "document_id, array_of_words"
    private static class LdaSrcFileMapper implements MapFunction<Tuple2<String, String>, Tuple2<String, String>> {
        @Override
        public Tuple2<String, String> map(Tuple2<String, String> arg0) throws Exception {
            return new Tuple2<String, String>(arg0.f0, arg0.f1);
        }
    }




    public static class TFComputer extends RichFlatMapFunction< Tuple2<Long, Tuple2<String, String>>, Tuple3<Long, String, Integer>> {

        // set of stop words
        private Set<String> stopWords;
        // map to count the frequency of words
        private transient Map<String, Integer> wordCounts;
        // pattern to match against words
        private transient Pattern wordPattern;

        public TFComputer() {
            this.stopWords = new HashSet<>();
        }

        public TFComputer(String[] stopWords) {
            // initialize stop words
            this.stopWords = new HashSet<>();
            Collections.addAll(this.stopWords, stopWords);
        }

        @Override
        public void open(Configuration config) {
            // initialized map and pattern
            this.wordPattern = Pattern.compile("(\\p{Alpha})+");
            this.wordCounts = new HashMap<>();
        }

        @Override
        public void flatMap(Tuple2<Long, Tuple2<String, String>> mail, Collector<Tuple3<Long ,String, Integer>> out) throws Exception {
            // clear count map
            this.wordCounts.clear();
            // tokenize mail body along whitespaces
            StringTokenizer st = new StringTokenizer(mail.f1.f1);
            // for each candidate word
            while(st.hasMoreTokens()) {
                // normalize to lower case
                String word = st.nextToken().toLowerCase();
                Matcher m = this.wordPattern.matcher(word);
                if(m.matches() && !this.stopWords.contains(word) && word.length()>1) {
                    // word matches pattern and is not a stop word -> increase word count
                    int count = 0;
                    if(wordCounts.containsKey(word)) {
                        count = wordCounts.get(word);
                    }
                    wordCounts.put(word, count + 1);
                }
            }
            // emit all counted words
            for(String word : this.wordCounts.keySet()) {
                out.collect(new Tuple3<>(mail.f0, word, this.wordCounts.get(word)));
            }
        }
    }

}
