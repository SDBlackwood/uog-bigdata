package models;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class CompositeKey implements WritableComparable<CompositeKey> {

    public enum KeyType {
        DOCUMENT('d'),
        TERM('t');

        private final char code;

        KeyType(final char code) {
            this.code = code;
        }

        public static KeyType fromCode(char code) {
            if (code == 'd') return KeyType.DOCUMENT;
            else return KeyType.TERM;
        }

        public char getCode() {
            return this.code;
        }

    }

    private KeyType keyType;
    private LongWritable documentId;
    private String term;

    // This constructor is needed for the framework
    public CompositeKey() { }

    public CompositeKey(String term) {
        this.keyType = KeyType.TERM;
        this.term = term;
        this.documentId = null;
    }

    public CompositeKey(LongWritable documentId) {
        this.keyType = KeyType.DOCUMENT;
        this.documentId = documentId;
        this.term = null;
    }

    public KeyType getKeyType() {
        return keyType;
    }

    public LongWritable getDocumentId() {
        return documentId;
    }

    public String getTerm() {
        return term;
    }


    public String toString() {
        StringBuilder output = new StringBuilder();
        output.append(this.keyType.code);
        output.append('|');
        if (this.keyType == KeyType.TERM) {
            output.append(this.term);
        } else {
            output.append(this.documentId);
        }
        return output.toString();
    }

    @Override
    public int compareTo(CompositeKey o) {
        // Documents come before terms, and the default ordering applies within categories
        if (this.getKeyType() == KeyType.DOCUMENT && o.getKeyType() == KeyType.TERM) {
            return -1;
        } else if (this.getKeyType() == KeyType.TERM && o.getKeyType() == KeyType.DOCUMENT) {
            return 1;
        } else if (this.getKeyType() == KeyType.TERM && o.getKeyType() == KeyType.TERM) {
            return this.getTerm().compareTo(o.getTerm());
        } else {
            return this.getDocumentId().compareTo(o.getDocumentId());
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeChar(this.getKeyType().getCode());
        if (this.getKeyType() == KeyType.DOCUMENT) {
            dataOutput.writeLong(this.getDocumentId().get());
        } else {
            try {
                dataOutput.writeUTF(this.getTerm());
            }catch (UTFDataFormatException e){
                // Truncate the term to 65535 - the maximum for Writable
                dataOutput.writeUTF(
                        new String(this.getTerm().getBytes(StandardCharsets.UTF_8), 0, 65535 - 2, StandardCharsets.UTF_8)
                );
            }
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.keyType = KeyType.fromCode(dataInput.readChar());
        if (this.keyType == KeyType.DOCUMENT) {
            this.documentId = new LongWritable(dataInput.readLong());
        } else if (this.keyType == KeyType.TERM) {
            this.term = dataInput.readUTF();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CompositeKey)) return false;
        CompositeKey that = (CompositeKey) o;
        return getKeyType() == that.getKeyType() &&
                Objects.equals(getDocumentId(), that.getDocumentId()) &&
                Objects.equals(getTerm(), that.getTerm());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getKeyType(), getDocumentId(), getTerm());
    }
}
