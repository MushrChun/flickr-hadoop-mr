package assign1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by MushrChun on 20/4/17.
 * input: locality-name, numofphotos
 */
public class Task1SortMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] dataArray = value.toString().split("\t");
        context.write(new Text(dataArray[0]), new Text(dataArray[1]));
    }
}
