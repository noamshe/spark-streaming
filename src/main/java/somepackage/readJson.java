package somepackage;

import com.google.gson.Gson;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.List;

public class readJson {
  public static void main(String[] args) {
    JavaStreamingContext jssc = new JavaStreamingContext("local", "JavaNetworkWordCount", new Duration(1000));
    JavaDStream<String> lines = jssc.textFileStream("/data/spark-streaming-input/");
    //JavaDStream<String> lines = jssc.textFileStream("hdfs://localhost:7000/dearnoam/");
    JavaDStream<String> words = lines.flatMap( new FlatMapFunction<String, String>() {
      @Override
      public Iterable<String> call(String x) {
        Gson gson = new Gson();
        User user = gson.fromJson( x, User.class );
        ArrayList<String> arr = new ArrayList<String>();
        arr.add(user.error);
        return arr;
      }
    });
    JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(String s) throws Exception {
        return new Tuple2<String, Integer>(s, 1);
      }
    });
    JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer i1, Integer i2) throws Exception {
        return i1 + i2;
      }
    });
    wordCounts.print();

    jssc.start();              // Start the computation
    jssc.awaitTermination();   // Wait for the computation to terminate
  }
}
class User {

  public String age = "29";
  public String name = "mkyong";
  public List<String> messages = new ArrayList<String>() {
    {
      add("msg 1");
      add("msg 2");
      add("msg 3");
    }
  };
  public String error = "";

  //getter and setter methods

  @Override
  public String toString() {
    return "User [age=" + age + ", name=" + name + ", " +
        "messages=" + messages + "]";
  }
}
