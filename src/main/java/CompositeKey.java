import org.apache.hadoop.io.LongWritable;

public class CompositeKey {

    enum KeyType {
        DOCUMENT,
        TERM
    }

    private KeyType keyType;
    private LongWritable documentId;
    private String term;

    public CompositeKey(KeyType keyType, LongWritable documentId, String term) {
        this.keyType = keyType;
        this.documentId = documentId;
        this.term = term;
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
}
