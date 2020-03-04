import org.junit.jupiter.api.Test;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TestSparkApp {

    @Test
    void justAnExample() {

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("WordCount-v0"));
        JavaRDD<String> _sc = sc.textFile("test.txt");

    }

}
