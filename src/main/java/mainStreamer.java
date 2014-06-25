import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;
import org.apache.spark.streaming.flume.*;

import java.util.regex.Pattern;

public class mainStreamer {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        Duration batchInterval = new Duration(5000);
        SparkConf sparkConf = new SparkConf().setAppName("JavaFlumeEventCount");
        sparkConf.setMaster("local[2]");

        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, batchInterval) ;
        JavaReceiverInputDStream<SparkFlumeEvent> flumeStream = FlumeUtils.createStream(ssc, "10.0.1.36", 9999);

        JavaDStream<String> words = flumeStream.flatMap(new FlatMapFunction<SparkFlumeEvent, String>() {
            @Override
            public Iterable<String> call(SparkFlumeEvent sparkFlumeEvent) throws Exception {
                Gson gson = new Gson();
                String ss = sparkFlumeEvent.event().toString();
                String typeFromHeader = ss.substring(ss.indexOf("type") + 8, ss.indexOf("}") - 1);
                String myJson = ss.substring(ss.indexOf("bytes") + 9, ss.length() - 3);
                TypeEnum type = TypeEnum.valueOf(typeFromHeader);
                String ccrid="0";

                String amount = "0";
                switch (type) {


                    case IMPRESSION:
                        ccrid=gson.fromJson(myJson,Params.class).params.ccrid;
                        break;

                    case CLICK:
                        ccrid=gson.fromJson(myJson,Params.class).params.ccrid;
                        break;

                    case CONVERSION:
                        Params requestParam1 = gson.fromJson(myJson, Params.class);
                        amount = requestParam1.params.payout;
                        ccrid=requestParam1.params.ccrid;
                        break;

                    case WIN:
                        Params requestParam2 = gson.fromJson(myJson, Params.class);
                        amount = requestParam2.params.price;
                        ccrid=requestParam2.params.ccrid;
                        break;
                    case BID:
                        ccrid=gson.fromJson(myJson,Param.class).ccrid;
                        break;

                    default:
                        amount = "0";
                }

                return Lists.newArrayList(ccrid + ":" + typeFromHeader + ":" + amount);
            }

        });
        JavaDStream<Object> wordCounts = words.mapToPair(
                new PairFunction<String, String, CloudObject>() {
                    @Override
                    public Tuple2<String, CloudObject> call(String s) {
                        String[] keyAndType = s.split(":");
                        TypeEnum type = TypeEnum.valueOf(keyAndType[1]);
                        String amount = keyAndType[2];

                        CloudObject cloudObject = new CloudObject();
                        switch (type) {
                            case IMPRESSION:
                                cloudObject.impression = 1;
                                break;
                            case BID:
                                cloudObject.bid = 1;
                                break;
                            case CONVERSION:
                                cloudObject.conversion = 1;
                                cloudObject.revenue = Double.valueOf(amount);
                                break;
                            case CLICK:
                                cloudObject.click = 1;
                                break;
                            case WIN:
                                cloudObject.cost = Double.valueOf(amount);
                                cloudObject.win = 1;
                                break;
                        }

                        return new Tuple2<String, CloudObject>(keyAndType[0], cloudObject);
                    }
                }).reduceByKey(new Function2<CloudObject, CloudObject, CloudObject>() {


            @Override
            public CloudObject call(CloudObject cloudObject1, CloudObject cloudObject2) throws Exception {

                return cloudObject1.add(cloudObject2);
            }
        }).flatMap(new FlatMapFunction<Tuple2<String, CloudObject>, Object>() {
            @Override
            public Iterable<Object> call(Tuple2<String, CloudObject> stringIntegerTuple2) throws Exception {
//        String[] keyAndType = stringIntegerTuple2._1().split(":");
//        TypeEnum type = TypeEnum.valueOf(keyAndType[1]);
//        String mysqlQuery = "insert into [1] (campaign_creative_id, [2], date) values (" + keyAndType[0] + "," + stringIntegerTuple2._2() + ", " + new Date().toString() + ") on duplicate key update [2] = [2] + " + stringIntegerTuple2._2();
//        switch (type) {
//          case IMPRESSION:
//            mysqlQuery.replace("[1]")
//            break;
//          case CLICK:
//            break;
//          default:
//        }


                // TBD write the output sqls
                return Lists.newArrayList((Object) (stringIntegerTuple2._1().toString() + " - " + ((CloudObject)stringIntegerTuple2._2()).toString()));
            }
        });


//        flumeStream.count().print();
        wordCounts.print();
        ssc.start();

        ssc.awaitTermination();
    }
}


