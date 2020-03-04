package mapreduce.wcv3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		Configuration myconf = getConf();

		// The following two lines instruct Hadoop/MapReduce to run in local
		// mode. In this mode, mappers/reducers are spawned as thread on the
		// local machine, and all URLs are mapped to files in the local disk.
		// Remove these lines when executing your code against the cluster.
		myconf.set("mapreduce.framework.name", "local");
        myconf.set("fs.defaultFS", "file:///");

		Job job = Job.getInstance(myconf);
		job.setJobName("MyWordCount(" + args[0] + ")");
		job.setJarByClass(WordCount.class);
		job.setInputFormatClass(MyInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(MyMapper.class);
		job.setPartitionerClass(MyPartitioner.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(MyReducer.class);
		job.setCombinerClass(MyReducer.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(job.getJobName() +
				"_output"));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new WordCount(), args));
	}
}