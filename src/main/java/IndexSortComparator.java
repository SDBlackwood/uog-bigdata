import org.apache.hadoop.io.RawComparator;

public class IndexSortComparator implements RawComparator {
    @Override
    public int compare(byte[] bytes, int i, int i1, byte[] bytes1, int i2, int i3) {
        return 0;
    }

    @Override
    public int compare(Object o1, Object o2) {
        return 0;
    }
}
