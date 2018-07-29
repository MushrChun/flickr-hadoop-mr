package assign1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by MushrChun on 20/4/17.
 * This mapper is for photo input
 */
public class Task3Job2Mapper2 extends Mapper<Object, Text, TextIntPair, Text>{
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] dataArray = value.toString().split("\t");

        // dataArray[4] is place_id dataArray[2] is tag
        context.write(new TextIntPair(dataArray[4],1), new Text(dataArray[2]+"\t"+dataArray[3]));

    }
}
