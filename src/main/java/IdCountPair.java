import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IdCountPair implements Writable {
    private LongWritable documentId;
    private int count;

    IdCountPair(LongWritable documentId, int count) {
        this.documentId = documentId;
        this.count = count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(documentId.get());
        dataOutput.writeInt(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.documentId = new LongWritable(dataInput.readLong());
        this.count = dataInput.readInt();
    }
}
