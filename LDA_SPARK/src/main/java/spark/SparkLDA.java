package spark;
import scala.Tuple2;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.DistributedLDAModel;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.SparkConf;

public class SparkLDA {
	 public static void main(String[] args) {
  //  SparkConf conf = new SparkConf().setAppName("LDA SPARK").setMaster("local").set("spark.ui.port", "4050");
				 SparkConf conf = new SparkConf().setAppName("LDA SPARK").set("spark.ui.port", "4050");  
		 @SuppressWarnings("resource")
			JavaSparkContext sc = new JavaSparkContext(conf);

		    // Load and parse the data
		    String path = "hdfs:///LDA_DATA/corpus";
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

		    // Cluster the documents into three topics using LDA
		    DistributedLDAModel ldaModel = (DistributedLDAModel)new LDA().setK(3).run(corpus);

		    // Output topics. Each is a distribution over words (matching word count vectors)
		    System.out.println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize()
		        + " words):");
		    
		    Matrix topics = ldaModel.topicsMatrix();
		    for (int topic = 0; topic < 3; topic++) {
		      System.out.print("Topic " + topic + ":");
		      for (int word = 0; word < ldaModel.vocabSize(); word++) {
		        System.out.print(" " + topics.apply(word, topic));
		      }
		      System.out.println();
		    }
		    sc.stop();
		  }
}