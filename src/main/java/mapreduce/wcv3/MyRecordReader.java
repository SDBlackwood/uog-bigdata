package mapreduce.wcv3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.clearspring.analytics.stream.membership.DataOutputBuffer;

public class MyRecordReader extends RecordReader<LongWritable, Text> {
	private static final byte[] recordSeparator = "\t\t\t".getBytes();
	private FSDataInputStream fsin;
	private long start, end;
	private boolean stillInChunk = true;
	private DataOutputBuffer buffer = new DataOutputBuffer();
	private LongWritable key = new LongWritable();
	private Text value = new Text();

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
		FileSplit split = (FileSplit) inputSplit;
		Configuration conf = context.getConfiguration();
		Path path = split.getPath();
		FileSystem fs = path.getFileSystem(conf);

		this.fsin = fs.open(path);
		fs.close();
		this.start = split.getStart();
		this.end = split.getStart() + split.getLength();
		this.fsin.seek(this.start);

		if (this.start != 0)
			readRecord(false);
	}

	private boolean readRecord(boolean withinBlock) throws IOException {
		int i = 0, b;
		while (true) {
			if ((b = this.fsin.read()) == -1)
				return false;
			if (withinBlock)
				this.buffer.write(b);
			if (b == recordSeparator[i]) {
				if (++i == recordSeparator.length)
					return this.fsin.getPos() < this.end;
			} else
				i = 0;
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException {
		if (!this.stillInChunk)
			return false;
		boolean status = readRecord(true);
		this.value = new Text();
		this.value.set(this.buffer.getData(), 0, this.buffer.getLength());
		this.key.set(this.fsin.getPos());
		this.buffer.reset();
		if (!status)
			this.stillInChunk = false;
		return true;
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