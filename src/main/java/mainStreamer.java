import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import java.util.Arrays;

public class mainStreamer {
  public static void main(String[] args) {
    JavaStreamingContext jssc = new JavaStreamingContext("local", "JavaNetworkWordCount", new Duration(1000));
    // Create a DStream that will connect to serverIP:serverPort, like localhost:9999
    JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
    // Split each line into words
    JavaDStream<String> words = lines.flatMap(
        new FlatMapFunction<String, String>() {
          @Override
          public Iterable<String> call(String x) {
            return Arrays.asList(x.split(" "));
          }
        });
    // Count each word in each batch
    JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
      @Override public Tuple2<String, Integer> call(String s) throws Exception {
        return new Tuple2<String, Integer>(s, 1);
      }
    });
    JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
      @Override public Integer call(Integer i1, Integer i2) throws Exception {
        return i1 + i2;
      }
    });
    wordCounts.print();

    jssc.start();              // Start the computation
    jssc.awaitTermination();   // Wait for the computation to terminate
  }
}
