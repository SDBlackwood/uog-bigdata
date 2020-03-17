import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class MyReducer extends Reducer<LongWritable, IdCountPair, LongWritable, Iterable<IdCountPair>> {

	@Override
	protected void reduce(LongWritable key, Iterable<IdCountPair> values, Context
			context) throws IOException, InterruptedException {

		context.write(key, values);
	}
}