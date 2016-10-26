package SEO;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Created by nina on 21.10.16.
 */
public class SEOReduser extends  Reducer<TextPair, IntWritable, TextPair, IntWritable> {
    // не выводить если кликов по запросу меньше чем nclicks_min
    int nclicks_min = 1;
    // текущий хост(хост, который сейчас обрабатывается)
    private Text curHost = null;
    // наиболее часто встречаемый запрос
    private Text bestQuery = null;
    // сколько раз встретился bestQuery
    private int maxCount;

    @Override
    public void setup(Context context) {
        curHost = null;
        bestQuery = null;
        nclicks_min = context.getConfiguration().getInt("seo.minclicks", nclicks_min);
    }
    @Override
    public void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException  {

        // инициализация для первого вызова
        if (curHost == null) {
            curHost = new Text(key.getFirst());
        }

        // если мы перешли к обработке следующего хоста
        if (key.getFirst().toString().compareTo(curHost.toString()) != 0) {
            if (maxCount >= nclicks_min) {
                context.write(new TextPair(curHost, bestQuery), new IntWritable(maxCount));
            }
            curHost = new Text(key.getFirst());
            bestQuery = null;
        }
        int count = 0;
        for (IntWritable value: values) {
            count += value.get();
        }
        if (bestQuery == null) {
            bestQuery = new Text(key.getSecond());
            maxCount = count;
        } else if (count > maxCount) {
            bestQuery = new Text(key.getSecond());
            maxCount = count;
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        if (curHost != null) {
            if (maxCount >= nclicks_min) {
                context.write(new TextPair(curHost, bestQuery), new IntWritable(maxCount));
            }
        }
    }
}
