package mapreduce.wcv2;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
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

		Job job = Job.getInstance(myconf, "WordCount-v3");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			if ("-skip".equals(args[i])) {
				job.addCacheFile(new Path(args[++i]).toUri());
				job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
			} else
				other_args.add(args[i]);
		}
		FileInputFormat.setInputPaths(job, new Path(other_args.get(0)));
		FileOutputFormat.setOutputPath(job, new Path(other_args.get(1)));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new WordCount(), args));
	}
}
