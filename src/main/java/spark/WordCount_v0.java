package spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCount_v0 {

	public static void main(String[] args) {
		Integer ONE = Integer.valueOf(1);
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("WordCount-v0"));
		JavaRDD<String> _sc = sc.textFile(args[0])
		.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
		_sc.mapToPair(s -> new Tuple2<String, Integer>(s, ONE))
		.reduceByKey((x, y) -> x + y)
		.saveAsTextFile(args[1]);
		sc.close();
	}
}
