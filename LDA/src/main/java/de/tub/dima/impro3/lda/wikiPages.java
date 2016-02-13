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
public class wikiPages {

  

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();



        DataSource<String> input = env.readTextFile(Config.pathToWikiPages());

        // read input with df-cut
        DataSet<Tuple2<String,String>> terms = input.flatMap(new DataReader());


        terms.writeAsCsv(Config.pathToWikiResults(), "\n", "\t", FileSystem.WriteMode.OVERWRITE);

        env.execute();
    }




   

    public static class DataReader implements FlatMapFunction<String, Tuple2<String,String >> {
        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
        public void flatMap(String line, Collector<Tuple2<String,String>> collector) throws Exception {

            String[] tokens = line.split(",");
            String cat = tokens[0].trim();
            String txt = "";
 
            for (int i = 1 ; i<tokens.length ;i++) {
            	if (!tokens[i].isEmpty() || !tokens[i].equals("")) {
            		
            		if (txt.equals(""))
            			txt= txt +tokens[i];	
            		else
            			
            		txt= txt+ "," +tokens[i];
            	}
               
            }
            
            collector.collect(new Tuple2<String, String>(cat ,txt));
        }

    }

    


}
