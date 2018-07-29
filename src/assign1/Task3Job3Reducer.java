package assign1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by MushrChun on 20/4/17.
 */
public class Task3Job3Reducer extends Reducer<Text,IntWritable, Text, Text>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        String[] keys = key.toString().split("\t");

        int count = 0;

        Iterator<IntWritable> valueItr = values.iterator();

        while(valueItr.hasNext()){
            count += valueItr.next().get();
        }
        context.write(new Text(keys[0]+"\t"+keys[1]) , new Text(keys[2]+"\t"+String.valueOf(count)));
    }
}
