import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroupComparator extends WritableComparator {

    public MyGroupComparator(){
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CompositeKey A = (CompositeKey) a;
        CompositeKey B = (CompositeKey) b;
        return A.compareTo(B);
    }
}
