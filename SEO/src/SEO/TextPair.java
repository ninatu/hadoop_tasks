package SEO;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by nina on 20.10.16.
 */
public class TextPair implements WritableComparable<TextPair> {
    private Text first;
    private Text second;

    TextPair() {
        set(new Text(), new Text());
    }

    TextPair(String first, String second) {
        set(new Text(first), new Text(second));
    }

    TextPair(Text first, Text second) {
        set(first, second);
    }

    //TextPair(TextPair tp) { set(new Text(tp.getFirst()), new Text(tp.getSecond())); }

    private void set(Text text1, Text text2) {
        first = text1;
        second = text2;
    }

    public Text getFirst() {
        return first;
    }
    public Text getSecond() {
        return second;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }

    @Override
    public int hashCode() {
        return first.hashCode() * 157 + second.hashCode();
    }

    @Override
    public int compareTo(@Nonnull TextPair tp) {
        int cmp = first.compareTo(tp.getFirst());
        if (cmp != 0) {
            return cmp;
        }
        return second.compareTo(tp.getSecond());
    }
	@Override
	public boolean equals(Object o) {
		if (o instanceof TextPair) {
			TextPair tp = (TextPair) o;
			return first.equals(tp.getFirst()) && second.equals(tp.getSecond());
		}
		return false;
	}
    @Override
    public String toString() {
        return first + "\t" + second;
    }
	
}
