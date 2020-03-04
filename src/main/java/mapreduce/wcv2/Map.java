package mapreduce.wcv2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
	static enum Counters { INPUT_WORDS }
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private boolean caseSensitive = true;
	private Set<String> patternsToSkip = new HashSet<String>();
	private long numRecords = 0;
	private String inputFile;

	private void parseSkipFile(Path patternsFile) {
		try {
			BufferedReader fis = new BufferedReader(new
					FileReader(patternsFile.toString()));
			String pattern = null;
			while ((pattern = fis.readLine()) != null)
				this.patternsToSkip.add(pattern);
			fis.close();
		} catch (IOException ioe) {
			System.err.println("Caught exception while parsing the cached file ’" +
					patternsFile + "’ : " + StringUtils.stringifyException(ioe));
		}
	}

	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		this.caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
		this.inputFile = conf.get("map.input.file");

		if (conf.getBoolean("wordcount.skip.patterns", false)) {
			URI[] patternsFiles = new URI[0];
			try {
				patternsFiles = context.getCacheFiles();
			} catch (IOException ioe) {
				System.err.println("Caught exception while getting cached files: " +
						StringUtils.stringifyException(ioe));
			}
			for (URI patternsFile : patternsFiles)
				parseSkipFile(new Path(patternsFile.getPath()));
		}
	}

	@Override
	public void cleanup(Context context) {
		this.patternsToSkip.clear();
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws
	IOException, InterruptedException {
		String line = this.caseSensitive ? value.toString() :
			value.toString().toLowerCase();

		for (String pattern : this.patternsToSkip)
			line = line.replaceAll(pattern, "");

		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) {
			this.word.set(tokenizer.nextToken());
			context.write(this.word, one);
			context.getCounter(Counters.INPUT_WORDS).increment(1);
		}

		if ((++this.numRecords % 100) == 0)
			context.setStatus("Finished processing " + this.numRecords + " records " +
					"from the input file: " + this.inputFile);
	}
}

