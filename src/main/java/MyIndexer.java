
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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

        Job job = Job.getInstance(myconf, "WikiIndex-v0");

        job.setJarByClass(MyIndexer.class);
        job.setInputFormatClass(MyInputFormat.class);
        job.setOutputFormatClass(MyOutputFormat.class);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(IdCountPair.class);
        job.setReducerClass(MyReducer.class);
        job.setCombinerClass(MyReducer.class);
//        job.setGroupingComparatorClass(MyGroupComparator.class);
//        job.setSortComparatorClass(MySortComparator.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //job.setNumReduceTasks(4);

        return (job.waitForCompletion(true) ? 0 : 1);
    }
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new MyIndexer(), args));
    }
}
