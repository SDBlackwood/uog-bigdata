import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class MyReducer extends Reducer<CompositeKey, IdCountPair, Text, Text> {

	@Override
	protected void reduce(CompositeKey key, Iterable<IdCountPair> values, Context
			context) throws IOException, InterruptedException {

		Map<LongWritable, Integer> outputPairs = new HashMap<>();

		for (IdCountPair pair : values) {
			int count = outputPairs.getOrDefault(pair.getDocumentId(), 0);
			outputPairs.put(pair.getDocumentId(), count + pair.getCount());
		}

		StringBuilder output = new StringBuilder();

		for (LongWritable documentId : outputPairs.keySet()) {
			output
					.append(documentId.toString())
					.append('|')
					.append(outputPairs.get(documentId))
					.append(" ");
		}
		context.write(new Text(key.toString()), new Text(output.toString()));
	}
}