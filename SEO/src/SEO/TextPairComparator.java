package SEO;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by nina on 21.10.16.
 */
public class TextPairComparator extends WritableComparator {
    TextPairComparator() {
        super(TextPair.class, true);
    }

    @Override
    public int compare(WritableComparable value1, WritableComparable value2) {
        TextPair tp1 = (TextPair) value1;
        TextPair tp2 = (TextPair) value2;
        return tp1.compareTo(tp2);
    }
}
