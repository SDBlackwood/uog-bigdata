package models;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class IdCountPair implements Writable {
    private LongWritable documentId;
    private int count;

    private static String stringSeparator = "|";

    public IdCountPair(LongWritable documentId, int count) {
        this.documentId = documentId;
        this.count = count;
    }

    public LongWritable getDocumentId() {
        return documentId;
    }

    public Integer getCount() {
        return count;
    }

    public IdCountPair() {}

    public String toString () {
        return documentId.toString() + stringSeparator + count;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IdCountPair)) return false;
        IdCountPair that = (IdCountPair) o;
        return count == that.count &&
                documentId.equals(that.documentId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(documentId, count);
    }
}
