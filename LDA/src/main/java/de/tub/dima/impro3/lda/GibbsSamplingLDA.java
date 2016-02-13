package de.tub.dima.impro3.lda;
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

        import org.apache.flink.api.java.ExecutionEnvironment;

        import org.apache.flink.api.java.DataSet;
        import org.apache.flink.api.java.tuple.Tuple2;
        import org.apache.flink.api.java.tuple.Tuple3;
        import org.apache.flink.configuration.Configuration;
        import org.apache.flink.core.fs.FileSystem.WriteMode;
        import org.apache.flink.util.Collector;

        import org.apache.flink.api.common.functions.MapFunction;
        import org.apache.flink.api.common.functions.RichMapFunction;
        import org.apache.flink.api.common.functions.FilterFunction;
        import org.apache.flink.api.common.functions.FlatMapFunction;
        import org.apache.flink.api.common.operators.Order;

        import java.util.ArrayList;
        import java.util.HashMap;
        import java.util.List;
        import java.util.Random;

public class GibbsSamplingLDA {

    public static final String documents_path = "/tmp/test.tab";
    public static final String pathTopicDistOnDoc = "/tmp/out/topicDistOnDoc";
    public static final String pathWordDistOnTopic = "/tmp/out/wordDistOnTopic";
    public static final int kTopic = 20;
    public static final double maxIter = 100;
    public static final double alpha = 0.45;
    public static final double beta = 0.01;
    public static Integer[][] nkv;
    public static Integer[] nk;
    public static int vSize;

    public static void main(String[] args) throws Exception {

        //Step1, set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        //Step2, read files into HDFS
	// ldasrcFile = DataSet<Tuple1<array of words in the document>>
	DataSet<Tuple1<String[]>> ldasrcFile = env.readCsvFile(documents_path)
						.lineDelimiter("\n")
						.fieldDelimiter("\t")
						.ignoreInvalidLines()
						.includeFields("01")
						.types(String.class)
						.filter(new LdaSrcFileFilter())
						.map(new LdaSrcFileMapper());

	// ldasrc0 = DataSet<Tuple2<documentID,Tuple1<array of words>>>
	DataSet<Tuple2<Long,Tuple1<String[]>>> ldasrc0 = DataSetUtils.zipWithUniqueId(ldasrcFile);
	
	// ldasrc = DataSet<Tuple2<documentID,array of words>>
	DataSet<Tuple2<Long,String[]>> ldasrc = ldasrc0.map(new LdaSrcWithUniqueIdFileMapper());

        //Step3, build a dictionary for alphabet: wordIndexMap
        // dictionary = DataSet<Tuple2<word,1>>, non-duplicate alphabetical sorted list of words
        DataSet<Tuple2<String,Integer>> dictionary = ldasrc.flatMap(new DictionaryBuilder())
                .distinct()
                .sortPartition(0, Order.ASCENDING);

        vSize = (int) dictionary.count();

        //Step4, initialize topic assignments for each word in the corpus (document_id, topicAssignArr, nmk)
        // documents = DataSet<Tuple3<documentid,List<Tuple2<wordID,topicID>>,List<count of words for each topicID>>>
        // wordID given sequentially to each of the words in dictionary -> first word of dictionary : ID = 1
        DataSet<Tuple3<Long,List<Tuple2<Integer,Integer>>,List<Integer>>> documents = ldasrc
                .map(new DocumentTopicsMapper())
                .withBroadcastSet(dictionary, "dictionary");

        // wordTopicsReduced = DataSet<Tuple2<wordID,topicID>, count> for all documents
        DataSet<Tuple2<Tuple2<Integer, Integer>,Integer>> wordTopicsReduced = documents
                .flatMap(new DocumentFlatMapper())
                .groupBy(0)
                .sum(1);

        //Initialize nk and nkv
        // nk = vector (size: number of topics) with the sum of counts in wordTopicsReduced for each topic
        // nkv = matrix (size: [number of topics][dictionary size] with the sum of counts in wordTopicsReduced for each [topic][word]
        nkv = new Integer[kTopic][vSize];
        nk = new Integer[kTopic];
        updateNKandNKV(wordTopicsReduced.collect(), vSize);

        //Step5, use gibbs sampling to infer the topic distribution in doc and estimate the parameter nkv and nk
        for (int iter=0;iter < maxIter; iter++) {
            documents = documents.map(new GibbsSamplingMapper());

            wordTopicsReduced = documents.flatMap(new DocumentFlatMapper())
                    .groupBy(0)
                    .sum(1);

            updateNKandNKV(wordTopicsReduced.collect(), vSize);
            System.out.println("Iteration " + iter + " finished");
        }

        //Step6, save the result in file system (part 1: words in each topic, part 2: topic distribution of documents)
        saveWordTopicDist(env,dictionary.collect());
        saveDocTopicDist(documents);

        // execute program
        env.execute();
    }

    private static void saveWordTopicDist(ExecutionEnvironment env, List<Tuple2<String,Integer>> dictionary) {

        List<Tuple2<Integer,List<Tuple2<String,Double>>>> wordDistPerTopic = new ArrayList<Tuple2<Integer,List<Tuple2<String,Double>>>>(kTopic);

        for (int v=0;v<kTopic;v++) {
            List<Tuple2<String,Double>> wordsInTopic = new ArrayList<Tuple2<String,Double>>(vSize);
            for (int w=0;w<vSize;w++) {
                wordsInTopic.add(w, new Tuple2<String,Double>(dictionary.get(w).f0,(double)nkv[v][w]/(double)nk[v]));
            }
            wordDistPerTopic.add(v, new Tuple2<Integer,List<Tuple2<String,Double>>>(v,wordsInTopic));
        }

        DataSet<Tuple2<Integer,List<Tuple2<String,Double>>>> wordDistPerTopic2 = env.fromCollection(wordDistPerTopic);
        wordDistPerTopic2.writeAsCsv(pathWordDistOnTopic,WriteMode.OVERWRITE);
    }

    @SuppressWarnings("serial")
    private static void saveDocTopicDist(DataSet<Tuple3<Long,List<Tuple2<Integer,Integer>>,List<Integer>>> documents) {

        documents.map(new MapFunction<Tuple3<Long,List<Tuple2<Integer,Integer>>,List<Integer>>,Tuple2<Long,List<Double>>>() {

            @Override
            public Tuple2<Long, List<Double>> map(Tuple3<Long, List<Tuple2<Integer, Integer>>, List<Integer>> arg0)
                    throws Exception {

                Double docLen = (double) arg0.f1.size();
                List<Double> probabilities = new ArrayList<Double>(arg0.f2.size());

                for (int i=0;i<arg0.f2.size();i++) {
                    probabilities.add(i, arg0.f2.get(i)/docLen);
                }

                return new Tuple2<Long,List<Double>>(arg0.f0,probabilities);
            }

        }).writeAsCsv(pathTopicDistOnDoc,WriteMode.OVERWRITE);
    }

    @SuppressWarnings("serial")
    private static class GibbsSamplingMapper implements MapFunction<Tuple3<Long,List<Tuple2<Integer,Integer>>,List<Integer>>,Tuple3<Long,List<Tuple2<Integer,Integer>>,List<Integer>>> {

        @Override
        public Tuple3<Long, List<Tuple2<Integer, Integer>>, List<Integer>> map(
                Tuple3<Long, List<Tuple2<Integer, Integer>>, List<Integer>> arg0) throws Exception {

            //Document's length
            int length = arg0.f1.size();

            //newNMK: list of occurrences per topic in document
            //newTopicAssignArr: list of Tuple2<wordID,topicID> in document
            List<Integer> newNMK = arg0.f2;
            List<Tuple2<Integer,Integer>> newTopicAsignArr = arg0.f1;

            for (int i=0;i<length;i++) {
                Integer topic = newTopicAsignArr.get(i).f1;
                Integer word = newTopicAsignArr.get(i).f0;

                //reset nkv, nv (global variables) and newNMK
                newNMK.set(topic, newNMK.get(topic) - 1);
                nkv[topic][word] -= 1;
                nk[topic] -= 1;

                //sampling
                double[] topicDist = new double[kTopic];

                for (int k=0;k<kTopic;k++) {
                    topicDist[k] = ((double)newNMK.get(k) + alpha) * (nkv[k][word] + beta) / (nk[k] + vSize * beta);
                }

                Integer newTopic = getRandFromMultinomial(topicDist);

                newTopicAsignArr.set(i, new Tuple2<Integer,Integer>(word,newTopic));
                newNMK.set(newTopic, newNMK.get(newTopic) + 1);
                nkv[newTopic][word] = nkv[newTopic][word] + 1;
                nk[newTopic] = nk[newTopic] + 1;
            }

            return new Tuple3<Long, List<Tuple2<Integer, Integer>>, List<Integer>>(arg0.f0,newTopicAsignArr,newNMK);
        }

    }

    private static Integer getRandFromMultinomial(double[] topicDist) {

        Random rd = new Random();

        Double rand = rd.nextDouble();
        Double sum = 0.0;
        for (double i : topicDist) { sum += i; }
        for (int i=0;i<topicDist.length;i++) {
            topicDist[i] = topicDist[i]/sum;
        }

        Double localSum = 0.0;
        int i;

        for (i=0;i<topicDist.length;i++) {
            localSum += topicDist[i];
            if (localSum >= rand) break;
        }

        return i;
    }


    //get nk vector
    //List(((0,0),2), ((0,1),1),((word,topic),count))
    //=> Array[]
    // get nkv matrix
    //List(((0,0),2), ((0,1),1),((word,topic),count))
    //=> Array[Array(...)]
    private static void updateNKandNKV(List<Tuple2<Tuple2<Integer, Integer>, Integer>> wordTopicsReduced, int vSize) {

        for (int i=0;i<kTopic;i++) {
            nk[i] = 0;
            for (int j=0;j<vSize;j++) {nkv[i][j] = 0;}
        }

        for (int w=0;w<wordTopicsReduced.size();w++) {
            nk[wordTopicsReduced.get(w).f0.f1] += wordTopicsReduced.get(w).f1;
            nkv[wordTopicsReduced.get(w).f0.f1][wordTopicsReduced.get(w).f0.f0] += wordTopicsReduced.get(w).f1;
        }
    }

    @SuppressWarnings("serial")
    public static class DocumentFlatMapper implements FlatMapFunction<Tuple3<Long,List<Tuple2<Integer,Integer>>,List<Integer>>,Tuple2<Tuple2<Integer,Integer>, Integer>> {

        @Override
        public void flatMap(Tuple3<Long, List<Tuple2<Integer, Integer>>, List<Integer>> arg0,
                            Collector<Tuple2<Tuple2<Integer, Integer>,Integer>> arg1) throws Exception {
            for (int i=0;i<arg0.f1.size();i++) {
                arg1.collect(new Tuple2<Tuple2<Integer,Integer>,Integer>(arg0.f1.get(i),1));
            }
        }
    }

    @SuppressWarnings("serial")
    public static class DocumentTopicsMapper extends RichMapFunction<Tuple2<Long,String[]>, Tuple3<Long,List<Tuple2<Integer,Integer>>,List<Integer>>>  {

        private HashMap<String,Integer> wordIndexMap;

        @Override
        public void open(Configuration parameters) throws Exception {

            List<Tuple2<String, Integer>> dictionaryList = new ArrayList<Tuple2<String, Integer>>(vSize);

            dictionaryList = getRuntimeContext().getBroadcastVariable("dictionary");

            this.wordIndexMap = new HashMap<String,Integer>(vSize);
            for (int i=0;i<dictionaryList.size();i++) {
                wordIndexMap.put(dictionaryList.get(i).f0, i);
            }
        }

        @Override
        public Tuple3<Long, List<Tuple2<Integer, Integer>>, List<Integer>> map(Tuple2<Long, String[]> arg0)
                throws Exception {

            int length = arg0.f1.length;
            List<Tuple2<Integer, Integer>> topicAssignArr = new ArrayList<Tuple2<Integer,Integer>>(length);
            List<Integer> nmk = new ArrayList<Integer>(kTopic);
            for (int j=0;j<kTopic;j++) {
                nmk.add(j, 0);
            }
            Random randomInt = new Random();

            for (int i=0;i<length;i++) {
                int topic = randomInt.nextInt(kTopic);
                topicAssignArr.add(i, new Tuple2<Integer,Integer>(wordIndexMap.get(arg0.f1[i]),topic));
                nmk.set(topic, nmk.get(topic) + 1);
            }

            return new Tuple3<Long, List<Tuple2<Integer, Integer>>, List<Integer>>(arg0.f0,topicAssignArr,nmk);
        }

    }

    @SuppressWarnings("serial")
    private static class DictionaryBuilder implements FlatMapFunction<Tuple2<Long,String[]>,Tuple2<String,Integer>> {
        @Override
        public void flatMap(Tuple2<Long, String[]> arg0, Collector<Tuple2<String, Integer>> arg1) throws Exception {
            String[] listOfWords = arg0.f1;
            for (int i=0;i<listOfWords.length;i++){
                arg1.collect(new Tuple2<String,Integer>(listOfWords[i],1));
            }
        }
    }

    @SuppressWarnings("serial")
	private static class LdaSrcFileFilter implements FilterFunction<Tuple1<String>> {
		@Override
		public boolean filter(Tuple1<String> tuple) throws Exception {
			return !tuple.f0.isEmpty() && tuple.f0.length() > 0;
		}
	}
	
	@SuppressWarnings("serial")
	// INPUT : "content"
	// OUTPUT: "array_of_words"
	private static class LdaSrcFileMapper implements MapFunction<Tuple1<String>, Tuple1<String[]>> {
		@Override
		public Tuple1<String[]> map(Tuple1<String> arg0) throws Exception {
			return new Tuple1<String[]>(arg0.f0.split(","));			
		}
	}

	@SuppressWarnings("serial")
	// INPUT : "document_id, Tuple1<array_of_words>"
	// OUTPUT: "document_id, array_of_words"
	private static class LdaSrcWithUniqueIdFileMapper implements MapFunction<Tuple2<Long,Tuple1<String[]>>, Tuple2<Long,String[]>> {
		@Override
		public Tuple2<Long, String[]> map(Tuple2<Long, Tuple1<String[]>> arg0) throws Exception {
			return new Tuple2<Long, String[]>(arg0.f0,arg0.f1.f0);
		}
	}

}
