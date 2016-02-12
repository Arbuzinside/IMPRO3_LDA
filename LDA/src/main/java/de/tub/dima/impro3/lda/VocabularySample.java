package de.tub.dima.impro3.lda;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

/**
 * Created by arbuzinside on 7.2.2016.
 */
public class VocabularySample {

  

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        env.setParallelism(1);

        DataSource<String> input = env.readTextFile(Config.pathToConditionals());
          DataSet< String> filteredTerms = input.flatMap(new DataReader());
        DataSet<Tuple2<Long, String>> vocabulary =DataSetUtils.zipWithUniqueId(filteredTerms).sortPartition(1, Order.ASCENDING);

    

        vocabulary.writeAsCsv("vocabNew2", "\n", "\t", FileSystem.WriteMode.OVERWRITE);



        env.execute();
    }


 


    public static class DataReader implements FlatMapFunction<String,  String> {
        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
        public void flatMap(String line, Collector< String> collector) throws Exception {

            String[] tokens = line.split("\t");
            
            tokens[1] = tokens[1].replace("(", "").replace(")", "");
            
            String[] txt = tokens[1].split(",");
            
            int count = Integer.parseInt(txt[1]);
            String vocab = txt[0];
            
            if (count >= 2000)
            
                collector.collect((vocab));
           
        }

    }

 
}
