package mapreduce.wcv3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable _value = new IntWritable();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context
			context) throws IOException, InterruptedException {
		int sum = 0;
		for (Iterator<IntWritable> it = values.iterator(); it.hasNext();)
			sum += it.next().get();
		this._value.set(sum);
		context.write(key, this._value);
	}
}