package de.tub.dima.impro3.lda;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.math.DenseVector;
import org.apache.flink.ml.math.Matrix;
import scala.Tuple3;

import java.util.ArrayList;
 
import java.util.HashSet;
 
import java.util.List;
import java.util.Set;


/**
 * Created by arbuzinside on 29.12.2015.
 */


public class LDA_Job2 {


    final static Set<String> sWords = new HashSet<String>();

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();



        String path = args[0];
        DataSet<String> rawLines = env.readTextFile(path);

        DataSet<Tuple2<Long, DenseVector>> corpus =  rawLines.map(new DataParser());
 
//        /**
//         * Default parameters:
//         *
//         *  S 1 4 16 64 256 1024 4096 16384     - mini batch size
//            κ 0.9 0.9 0.8 0.7 0.6 0.5 0.5 0.5   - learning rate
//            τ0 1024 1024 1024 1024 1024 1024 64 1 - early iterations
//         *
//         *
//         */
//
//        int numberOfTopics = Integer.parseInt(args[2]);
//
//
//        LDAModel ldaModel = new LDA().setK(3).run(corpus);
//
//        // Output topics. Each is a distribution over words (matching word count vectors)
////        System.out.println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize()
////                + " words):");
//        Matrix topics = ldaModel.getTopics();
//      
//        List< Tuple3<Integer,Integer, Double>> list = new ArrayList< Tuple3<Integer,Integer, Double>>();
//		   
//	    for (int topic = 0; topic < numberOfTopics; topic++) {
// 
//	      for (int word = 0; word < ldaModel.vocabSize(); word++) {
//	       double v = topics.apply( topic,word);
//	      
//	       if (v!= 0.0) {
//	    	   list.add(new Tuple3<Integer,Integer, Double>(topic,word,v));
// 
//	       }
//	    	 
//	      }
//	    }
//
// 
// 
//	    
//	    DataSet<Tuple3<Integer, Integer, Double>> ds = env.fromCollection(list);
//	    ds.writeAsText( args[1]);
        corpus.print();
	 
       env.execute("LDA Test");
    }


    /**
     * Map parser to split lines into vectors with unique ids
     */
    public static class DataParser implements MapFunction<String, Tuple2<Long, DenseVector>> {

        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;
 
        @Override
        public Tuple2<Long,DenseVector> map(String s){
        	
        	
        	String[] sarray = s.trim().split(",");
			
            Long key = Long.parseLong(sarray[0].replace("(", ""));
            String[] array  = sarray[1].split("DenseVector");
            sarray[1] =  array[1].replace("(", "");
            
            sarray[sarray.length-1] = sarray[sarray.length-1].replace(")", "");	 
            double[] values = new double[sarray.length-1];
           for (int i = 1; i < sarray.length-1; i++) {
              values[i-1] = Double.parseDouble(sarray[i]);
            }
            return  new  Tuple2<Long, DenseVector> (key, new DenseVector(values));
        
        }
    }




}
