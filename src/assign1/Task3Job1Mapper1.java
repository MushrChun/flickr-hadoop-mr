package assign1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by MushrChun on 20/4/17.
 * This mapper is for Task2 result (locality, numofphotos)
 */
public class Task3Job1Mapper1 extends Mapper<Object, Text, TextIntPair, Text>{
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] dataArray = value.toString().split("\t");

        context.write(new TextIntPair(dataArray[0],0), new Text(dataArray[1]));

    }
}
