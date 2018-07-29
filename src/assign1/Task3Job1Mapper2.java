package assign1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by MushrChun on 20/4/17.
 * This Mapper is for task1tmp files which holds data as (locality, place_id)
 */
public class Task3Job1Mapper2 extends Mapper<Object, Text, TextIntPair, Text>{
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] dataArray = value.toString().split("\t");

        context.write(new TextIntPair(dataArray[0],1), new Text(dataArray[1]));

    }
}
