package spark;
import scala.Function1;
import scala.Tuple2;
import scala.Tuple3;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.DistributedLDAModel;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

public class SparkLDA {
	 public static void main(String[] args) {
 	 //  SparkConf conf = new SparkConf().setAppName("LDA SPARK").setMaster("local");
			 SparkConf conf = new SparkConf().setAppName("LDA SPARK");
		 @SuppressWarnings("resource")
			JavaSparkContext sc = new JavaSparkContext(conf);

		    // Load and parse the data
     	    String path = args[0];
		    JavaRDD<String> data = sc.textFile(path);
		    JavaRDD<Tuple2<Long, Vector>> parsedData = data.map(
		        new Function<String,  Tuple2<Long, Vector>>() {
		          /**
					 * 
					 */
					private static final long serialVersionUID = 1L;

				public  Tuple2<Long, Vector> call(String s) {
					String[] sarray = s.trim().split(",");
					
		            Long key = Long.parseLong(sarray[0].replace("(", ""));
		            String[] array  = sarray[1].split("DenseVector");
		            sarray[1] =  array[1].replace("(", "");
		            
		            sarray[sarray.length-1] = sarray[sarray.length-1].replace(")", "");	 
		            double[] values = new double[sarray.length-1];
                   for (int i = 1; i < sarray.length-1; i++) {
		              values[i-1] = Double.parseDouble(sarray[i]);
		            }
		            return  new  Tuple2<Long, Vector> (key, Vectors.dense(values));

		          }
		        }
		    );
		    
//		    // Index documents with unique IDs
		    JavaPairRDD<Long, Vector> corpus = JavaPairRDD.fromJavaRDD(parsedData.map(
		        new Function<Tuple2<Long, Vector>, Tuple2<Long, Vector>>() {
		          /**
					 * 
					 */
					private static final long serialVersionUID = 1L;

				public Tuple2<Long, Vector> call(Tuple2<Long, Vector> doc_id) {
		            return doc_id;
		          }
		        }
		    ));
		    corpus.cache();
		    int k = 3;

		    // Cluster the documents into three topics using LDA
		    DistributedLDAModel ldaModel = (DistributedLDAModel)new LDA().setK(k).run(corpus);

		    // Output topics. Each is a distribution over words (matching word count vectors)
		    System.out.println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize()
		        + " words):");
		    Matrix topics = ldaModel.topicsMatrix();
				   
		   List< Tuple3<Integer,Integer, Double>> list = new ArrayList< Tuple3<Integer,Integer, Double>>();
		   
		    for (int topic = 0; topic < k; topic++) {
//		      System.out.print("Topic " + topic + ":");
		      for (int word = 0; word < ldaModel.vocabSize(); word++) {
		       double v = topics.apply(word, topic);
		       if (v!= 0.0) {
		    	   list.add(new Tuple3<Integer,Integer, Double>(topic,word,v));
//		    	   System.out.println(topic + " ///////"+ word + "_____"+v);
		    	   
		       }
		    	 
		      }
		    }
		    
		  sc.parallelize(list).saveAsTextFile(args[1]);	    
		    sc.stop();
		  }
	 
}