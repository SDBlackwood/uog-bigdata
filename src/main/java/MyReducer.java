import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class MyReducer extends Reducer<CompositeKey, IdCountPair, Text, Text> {

	@Override
	protected void reduce(CompositeKey key, Iterable<IdCountPair> values, Context
			context) throws IOException, InterruptedException {
		StringBuilder output = new StringBuilder();

		for (IdCountPair pair : values) {
			output.append(pair.toString()).append(" ");
		}
		context.write(new Text(key.toString()), new Text(output.toString()));
	}
}