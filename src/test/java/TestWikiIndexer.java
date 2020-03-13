
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import java.io.File;

public class TestWikiIndexer {
    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {

    }

    @Test
    public void testRecordReader() throws IOException, InterruptedException {
        Configuration conf = new Configuration(false);
        conf.set("fs.default.name", "file:///");

        File testFile = new File("src/test/java/test.txt");
        Path path = new Path(testFile.getAbsoluteFile().toURI());
        FileSplit split = new FileSplit(path, 0, testFile.length(), null);

        InputFormat inputFormat = ReflectionUtils.newInstance(MyInputFormat.class, conf);
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        RecordReader reader = inputFormat.createRecordReader(split, context);

        reader.initialize(split, context);
        reader.nextKeyValue();

        Text expected = new Text(new String ("[[A]]\nA Test\n"));
        Assert.assertEquals(new LongWritable(14),reader.getCurrentKey());
        Assert.assertEquals(expected,reader.getCurrentValue());

    }
    @Test
    public void testSeperator() throws IOException, InterruptedException {
       final byte[] recordSeparator = "[[".getBytes();
       System.out.println(new String(recordSeparator));
       Assert.assertEquals(91,recordSeparator[0]);
       Assert.assertEquals(91,recordSeparator[1]);
    }

}