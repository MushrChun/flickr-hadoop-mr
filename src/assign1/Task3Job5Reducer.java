package assign1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by MushrChun on 20/4/17.
 */
public class Task3Job5Reducer extends Reducer<IntWritable,Text, Text, Text>{

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Iterator<Text> valueItr = values.iterator();

        String[] arr = valueItr.next().toString().split("\t");

        context.write(new Text(arr[0]+"\t"+key.get()), new Text(arr[1]));

    }
}
