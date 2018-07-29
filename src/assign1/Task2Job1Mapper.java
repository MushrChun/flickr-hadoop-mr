package assign1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by cshe6391 on 4/19/2017.
 * input: place-name, numofphotos
 */
public class Task2Job1Mapper extends Mapper<Object, Text, IntWritable, Text> {

    private Text locality = new Text();
    private IntWritable count = new IntWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] dataArray = value.toString().split("\t");
        locality.set(dataArray[0]);
        count.set(Integer.parseInt(dataArray[1]));
        context.write(count,locality);
    }
}
