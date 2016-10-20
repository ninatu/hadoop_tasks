package SEO;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Created by nina on 21.10.16.
 */
public class SEOReduser extends  Reducer<TextPair, NullWritable, Text, Text> {
    // текущий хост(хост, который сейчас обрабатывается)
    private Text curHost = null;
    // текущая пара: хост + запрос
    private TextPair curTextPair = null;
    // сколько раз встретилось curTextPair
    private int count;
    // пара соотвествующая текущему хосту и наиболее часто встречаемому запросу
    private TextPair bestTextPair = null;
    // сколько раз встретилось bestTextPair
    private int maxCount;

    @Override
    public void reduce(TextPair key, Iterable<NullWritable> v, Context context) throws IOException, InterruptedException  {
        // инициализация для первого вызова
        if (curHost == null) {
            curHost = key.getFirst();
            curTextPair = key;
            count = 1;
            return;
        }

        // если это тот же хост
        if (key.getFirst().compareTo(curHost) == 0) {
            // если это тот же запрос
            if (curTextPair.compareTo(key) == 0) {
                count++;
            } else {
                if (bestTextPair == null) {
                    bestTextPair = curTextPair;
                    maxCount = count;
                }
                curTextPair = key;
                count = 1;
            }
        } else {
            // выводим хост и лучший запрос
            context.write(bestTextPair.getFirst(), bestTextPair.getSecond());
            curHost = key.getFirst();
            curTextPair = key;
            count = 1;
            bestTextPair = null;
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        if (curHost.compareTo(null) != 0) {
            context.write(bestTextPair.getFirst(), bestTextPair.getSecond());
        }
    }



}
