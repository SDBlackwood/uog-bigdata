import com.clearspring.analytics.stream.membership.DataOutputBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;


public class MyRecordReader extends RecordReader<LongWritable, Text> {

	private static final byte[] recordSeparator = "[[".getBytes();
	private FSDataInputStream fsin;
	private long start, end;
	private long position;
	private boolean stillInChunk = true;
	private DataOutputBuffer buffer = new DataOutputBuffer();
	private LongWritable key = new LongWritable();
	private Text value = new Text();
	private LineReader lineReader;


	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {

		Configuration job = context.getConfiguration();

		FileSplit split = (FileSplit) inputSplit;
		Configuration conf = context.getConfiguration();
		Path path = split.getPath();
		FileSystem fs = path.getFileSystem(conf);

		this.fsin = fs.open(path);
		fs.close();
		this.start = split.getStart();
		this.end = split.getStart() + split.getLength();
		this.fsin.seek(this.start);
		this.position = this.start;
		this.lineReader = new LineReader(this.fsin, conf, recordSeparator);

		if (this.start != 0)
			readRecord(false);
	}

	private boolean readRecord(boolean withinBlock) throws IOException {
		int articles = 0, symbolCount = 0, b;
		int[] temp = new int[3];

		while (true) {

			b = this.fsin.read();

			if (b == -1)
				return false;
			if (b == recordSeparator[symbolCount] && withinBlock){
				if (articles < 1){
					this.buffer.write(b);
				}else{
					articles = 0;
					return false;
				}
				if (++symbolCount == recordSeparator.length){
					symbolCount = 0;
					articles++;
				}
			}else{
				this.buffer.write(b);
			}


		}
	}

	@Override
	public boolean nextKeyValue() throws IOException {

		if (!this.stillInChunk)
			return false;
		boolean status = readRecord(true);
		this.value = new Text();
		this.key.set(this.fsin.getPos());
		this.value.set(this.buffer.getData(), 0, this.buffer.getLength());
		this.buffer.reset();
		if (!status)
			this.stillInChunk = false;
		return false;
	}

	@Override
	public LongWritable getCurrentKey() { return this.key; }

	@Override
	public Text getCurrentValue() { return this.value; }

	@Override
	public float getProgress() throws IOException {
		return (float) (this.fsin.getPos() - this.start) / (this.end - this.start);
	}

	@Override
	public void close() throws IOException { this.fsin.close(); }
}