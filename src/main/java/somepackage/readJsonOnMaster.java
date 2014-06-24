package somepackage;

import com.google.gson.Gson;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.List;

public class readJsonOnMaster {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("appName").setMaster("spark://linux-noam:7077");
    JavaSparkContext sc = new JavaSparkContext(conf);
  }
}
