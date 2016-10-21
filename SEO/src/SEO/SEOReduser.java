package SEO;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Created by nina on 21.10.16.
 */
public class SEOReduser extends  Reducer<TextPair, IntWritable, Text, Text> {
    // текущий хост(хост, который сейчас обрабатывается)
    private Text curHost = null;
    // наиболее часто встречаемый запрос
    private Text bestQuery = null;
    // сколько раз встретился bestQuery
    private int maxCount;

    @Override
    public void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException  {
        // инициализация для первого вызова
        if (curHost == null) {
            curHost = key.getFirst();
        }

        // если мы перешли к обработке следующего хоста
        if (!key.getFirst().equals(curHost)) {
            context.write(curHost, bestQuery);
            curHost = key.getFirst();
            bestQuery = null;
        }
        int count = 0;
        for (IntWritable value: values) {
            count += value.get();
        }
        if (bestQuery == null) {
            bestQuery = key.getSecond();
            maxCount = count;
        } else if (count > maxCount) {
            bestQuery = key.getSecond();
            maxCount = count;
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        if (curHost != null) {
    		context.write(curHost, bestQuery);
		}
    }
}
