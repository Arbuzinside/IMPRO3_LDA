package de.tub.dima.impro3.lda;


 
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.JoinOperator.ProjectJoin;
import org.apache.flink.api.java.operators.SortPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple;
 
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
 
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

 
 

 
public class MatrixParser {


    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        
        env.setParallelism(1);
        
        
        DataSource<String> data = env.readTextFile(Config.pathToSparkMatrix());
        DataSet<Tuple3<Integer,Integer, Double>> input =  data.flatMap(new DataReader());
        
        DataSet<Tuple2<Integer, String>> vocabulaty = env.readCsvFile(Config.pathToConditionals())
                .fieldDelimiter("\t")
                .lineDelimiter("\n")
                .types(Integer.class, String.class);
        
         ProjectJoin<Tuple3<Integer, Integer, Double>, Tuple2<Integer, String>, Tuple> join = input.join(vocabulaty).where(1).equalTo(0).projectFirst(0,1,2).projectSecond(1);
         
          SortPartitionOperator<Tuple> result = join.sortPartition(0, Order.ASCENDING).sortPartition(2, Order.DESCENDING);

          result.writeAsCsv(Config.pathToOutSpark(), "\n", "\t", FileSystem.WriteMode.OVERWRITE);



        env.execute();
    }
    
    
    
    public static class DataReader implements FlatMapFunction<String, Tuple3<Integer,Integer, Double>> {
        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
        public void flatMap(String line, Collector< Tuple3<Integer,Integer, Double>> collector) throws Exception {

			line = line.replace("(", "").replace(")", "");
            String[] tokens = line.split(",");
  
 
                collector.collect(new Tuple3<Integer,Integer, Double>(Integer.parseInt(tokens[0]),Integer.parseInt(tokens[1]),Double.parseDouble(tokens[2])));

        }

    }

}