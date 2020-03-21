import models.CompositeKey;
import models.IdCountPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyIndexer extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration myconf = getConf();

        // The following two lines instruct Hadoop/MapReduce to run in local
        // mode. In this mode, mappers/reducers are spawned as thread on the
        // local machine, and all URLs are mapped to files in the local disk.
        // Remove these lines when executing your code against the cluster.
        myconf.set("mapreduce.framework.name", "local");
        myconf.set("fs.defaultFS", "file:///");

        myconf.set("textinputformat.record.delimiter", "\n[[");

        Job job = Job.getInstance(myconf, "WikiIndex-v0");

        job.setJarByClass(MyIndexer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(IndexMapper.class);
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(IdCountPair.class);
        job.setReducerClass(IndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        MultipleOutputs.addNamedOutput(job, "documents", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "terms", TextOutputFormat.class, Text.class, Text.class);

        job.setNumReduceTasks(1);

        return (job.waitForCompletion(true) ? 0 : 1);
    }
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new MyIndexer(), args));
    }
}
