import models.CompositeKey;
import models.IdCountPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class IndexReducer extends Reducer<CompositeKey, IdCountPair, Text, Text> {

	private MultipleOutputs<Text, Text> multipleOutputs;

	public void setup(Context context) {
		multipleOutputs = new MultipleOutputs<>(context);
	}

	@Override
	protected void reduce(CompositeKey key, Iterable<IdCountPair> values, Context
			context) throws IOException, InterruptedException {

		// We use a slightly different output format and a different output file depending on the key type
		if (key.getKeyType() == CompositeKey.KeyType.TERM && !key.getTerm().equals("")) {
			this.reduceTerm(key.getTerm(), values);
		} else if (key.getKeyType() == CompositeKey.KeyType.DOCUMENT) {
			this.reduceDocument(key.getDocumentId(), values.iterator().next().getCount());
		}
	}

	private void reduceDocument(LongWritable documentId, Integer count) throws IOException, InterruptedException {
		// Don't output the document ID twice
		multipleOutputs.write("documents", new Text(documentId.toString()), new Text(count.toString()));
	}

	private void reduceTerm(String term, Iterable<IdCountPair> values) throws IOException, InterruptedException {

		StringBuilder output = new StringBuilder();

		for (IdCountPair pair : values) {
			output
					.append(pair.toString())
					.append(" ");
		}
		multipleOutputs.write("terms", new Text(term), new Text(output.toString()));
	}

	public void cleanup(Context context) throws IOException, InterruptedException {
		multipleOutputs.close();
	}
}