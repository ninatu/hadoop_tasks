import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WebPagesSortComporator extends WritableComparator {
    protected WebPagesSortComporator() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        Text key1 = (Text) w1;
        Text key2 = (Text) w2;
        int cmp = key1.toString().substring(1).compareTo(key2.toString().substring(1));
        if (cmp != 0) {
            return cmp;
        } else {
            return key1.toString().substring(0, 1).compareTo(key2.toString().substring(0, 1));
        }
    }
}