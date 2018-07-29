package assign1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by MushrChun on 20/4/17.
 */
public class Task1SortReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> valuesItr = values.iterator();
        int count = 0;
        while(valuesItr.hasNext()){
            count += Integer.parseInt(valuesItr.next().toString());
        }
        context.write(key, new Text(String.valueOf(count)));
    }
}
