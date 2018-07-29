package assign1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by MushrChun on 20/4/17.
 * This mapper is for Task3 Job1 result (locality, numofphotos, place_id)
 */
public class Task3Job2Mapper1 extends Mapper<Object, Text, TextIntPair, Text>{
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] dataArray = value.toString().split("\t");

        context.write(new TextIntPair(dataArray[2],0), new Text(dataArray[0]+"\t"+dataArray[1]));

    }
}
