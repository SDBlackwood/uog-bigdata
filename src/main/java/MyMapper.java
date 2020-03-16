import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.PorterStemmer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class MyMapper extends Mapper<LongWritable, Text, CompositeKey, IdCountPair> {

	static enum Counters { NUM_DOCUMENTS, TOTAL_TOKENS }
	Set<String> stopwords = new HashSet<>();
	PorterStemmer porterStemmer = new PorterStemmer();

	@Override
	protected void setup(Context context) throws IOException {
		BufferedReader bufferedReader = new BufferedReader(new FileReader("stopword-list.txt"));
		String word;

		while((word = bufferedReader.readLine()) != null) {
			this.stopwords.add(word);
		}
		bufferedReader.close();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer tokenizer = new StringTokenizer(value.toString());

		Map<CompositeKey, Integer> termCounts = new HashMap<>();
		int documentLength = 0;

		while (tokenizer.hasMoreTokens()) {
			String token = tokenizer.nextToken();

			String processedWord = this.processWord(token);
			if (processedWord == null) continue;

			CompositeKey termKey = new CompositeKey(processedWord);

			int count = termCounts.getOrDefault(termKey, 0);
			termCounts.put(termKey, count + 1);

			documentLength++;

		}

		context.getCounter(Counters.TOTAL_TOKENS).increment(documentLength);
		context.getCounter(Counters.NUM_DOCUMENTS).increment(1);

		context.write(new CompositeKey(key), new IdCountPair(key, documentLength));

		for (CompositeKey termKey : termCounts.keySet()) {
			context.write(termKey, new IdCountPair(key, termCounts.get(termKey)));
		}
	}

	private String processWord(String word) {
		if (this.stopwords.contains(word)) {
			return null;
		}

		return this.porterStemmer.stem(this.stripWord(word.toLowerCase()));
	}

	private String stripWord(String word) {
		return word.replaceAll("[^A-Za-z0-9]", "");
	}
}
