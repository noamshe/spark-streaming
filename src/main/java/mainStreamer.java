import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;
import org.apache.spark.streaming.flume.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

public class mainStreamer {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) {
    Duration batchInterval = new Duration(12000);
    SparkConf sparkConf = new SparkConf().setAppName("JavaFlumeEventCount");
    sparkConf.setMaster("local[2]");

    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, batchInterval) ;
    JavaReceiverInputDStream<SparkFlumeEvent> flumeStream = FlumeUtils.createStream(ssc, "10.0.2.152", 9999);

    JavaDStream<String> words = flumeStream.flatMap(new FlatMapFunction<SparkFlumeEvent, String>() {
      @Override
      public Iterable<String> call(SparkFlumeEvent sparkFlumeEvent) throws Exception {
        Gson gson = new Gson();
        String ss = sparkFlumeEvent.event().toString();
        String myJson = ss.substring(ss.indexOf("bytes") + 9,ss.length()-3);
        //AvroJson avro = gson.fromJson( ss, AvroJson.class );
        Params requestParam = gson.fromJson(myJson, Params.class);
        return Lists.newArrayList(requestParam.params.campaign_id + ":" + TypeEnum.IMP);
      }

    });
    JavaDStream<Object> wordCounts = words.mapToPair(
        new PairFunction<String, String, Integer>() {
          @Override
          public Tuple2<String, Integer> call(String s) {
            return new Tuple2<String, Integer>(s, 1);
          }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer i1, Integer i2) {
        return i1 + i2;
      }
    }).flatMap(new FlatMapFunction<Tuple2<String, Integer>, Object>() {
      @Override
      public Iterable<Object> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
        String[] keyAndType = stringIntegerTuple2._1().split(":");
        TypeEnum type = TypeEnum.valueOf(keyAndType[1]);
        String mysqlQuery = "insert into [1] (campaign_creative_id, [2], date) values (" + keyAndType[0] + "," + stringIntegerTuple2._2() + ", " + new Date().toString() + ") on duplicate key update [2] = [2] + " + stringIntegerTuple2._2();
        switch (type) {
          case IMP:
            mysqlQuery.replace("[1]")
            break;
          case CLICK:
            break;
          default:
        }
        return Lists.newArrayList((Object) (mysqlQuery));
      }
    });


//        flumeStream.count().print();
    wordCounts.print();
    ssc.start();

    ssc.awaitTermination();
  }
}


