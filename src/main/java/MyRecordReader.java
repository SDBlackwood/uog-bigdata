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
import org.apache.hadoop.util.LineReader;
import java.util.regex.Pattern;
import java.io.IOException;
import com.clearspring.analytics.stream.membership.DataOutputBuffer;

public class MyRecordReader extends RecordReader<LongWritable, Text> {

	final String pattern = ".*\\[\\[.*\\]\\].*";
	private LongWritable key = new LongWritable();
	private Text value = new Text();
	private FSDataInputStream fsin;
	private LineReader lineReader;
	private DataOutputBuffer buffer = new DataOutputBuffer();
	private boolean isRecordFinished = false;
	private long position = 0;
	private int isEnd = 0;
	private long start,end;

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {

		FileSplit split = (FileSplit) inputSplit;
		Configuration conf = context.getConfiguration();
		Path path = split.getPath();
		FileSystem fs = path.getFileSystem(conf);
		this.fsin = fs.open(path);
		this.lineReader = new LineReader(this.fsin, conf);
		this.start = split.getStart();
		this.end = split.getStart() + split.getLength();
		fs.close();
	}

	/**
	 *
	 * @return
	 * @throws IOException
	 */
	@Override
	public boolean nextKeyValue() throws IOException {


		boolean skipFirst = true;

		while(!isRecordFinished){
			Text content = new Text();

			// Skip the first line as we want to stop at the end
			// of the second article title e.g [[Title]]
			if(skipFirst){
				this.lineReader.readLine(content);
				this.buffer.write(content.getBytes());
				//  add a new line as it removed in the linereadedr
				this.buffer.write("\n".getBytes());
				skipFirst = false;
				continue;
			}

			isEnd = this.lineReader.readLine(content);

			if (isEnd == -1){
				// We have reached end of input
				// Return false to stop processing this split
				this.fsin.close();
				this.buffer.reset();
				return false;

				// TODO: There is an edge case here where a record
				// is between the end of one split and the start of
				// another.  Need to handle for this.
			}

			// If we find a match then we want to emit the position
			// We got up to previously, not the current line
			this.isRecordFinished = Pattern.matches(pattern, content.toString());
			if(!this.isRecordFinished){
				this.buffer.write(content.getBytes());
				this.buffer.write("\n".getBytes());
				position = this.fsin.getPos();
			}
		}

		this.key.set(position);
		this.value.set(this.buffer.getData());
		this.buffer.reset();
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