package assign1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by MushrChun on 20/4/17.
 * This mapper is for Task3 Job4 result
 */
public class Task3Job5Mapper extends Mapper<Object, Text, IntWritable, Text>{
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] dataArray = value.toString().split("\t");
        String numOfPhoto = dataArray[1];
        context.write(new IntWritable(Integer.parseInt(numOfPhoto)), new Text(dataArray[0]+"\t"+dataArray[2]));

    }
}
