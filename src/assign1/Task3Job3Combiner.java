package assign1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by MushrChun on 20/4/17.
 */
public class Task3Job3Combiner extends Reducer<Text,IntWritable, Text, IntWritable>{

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {


        int count = 0;

        Iterator<IntWritable> valueItr = values.iterator();

        while(valueItr.hasNext()){
            count += valueItr.next().get();
        }
        context.write(new Text(key) , new IntWritable(count));
    }
}
