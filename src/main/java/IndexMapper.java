import models.CompositeKey;
import models.IdCountPair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.PorterStemmer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import org.apache.hadoop.fs.FileSystem;

public class IndexMapper extends Mapper<LongWritable, Text, CompositeKey, IdCountPair> {

	static enum Counters { NUM_DOCUMENTS, TOTAL_TOKENS }
	Set<String> stopwords = new HashSet<>();
	PorterStemmer porterStemmer = new PorterStemmer();

	@Override
	protected void setup(Context context) throws IOException {

		//String pathString = "hdfs://bigdata-10.dcs.gla.ac.uk:8022/user/2092282b/stopword-list.txt";
		String pathString = "src/main/resources/stopword-list.txt";
		Path path = new Path(pathString);
		FileSystem fs = FileSystem.get(context.getConfiguration());
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(path)));

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
		String normalisedWord = this.stripWord(word.toLowerCase());
		if (this.stopwords.contains(normalisedWord)) {
			return null;
		}

		return this.porterStemmer.stem(normalisedWord);
	}

	private String stripWord(String word) {
		return word.replaceAll("[^A-Za-z0-9]", "");
	}
}
