package assign1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by MushrChun on 19/4/17.
 * Input:  place-name, place-id
 */
public class Task1PlaceMergeMapper extends Mapper<Object, Text, TextIntPair, Text>{


    private TextIntPair tip = new TextIntPair();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] dataArray = value.toString().split("\t");

        tip.setKey(new Text(dataArray[1]));
        tip.setOrder(new IntWritable(0));
        context.write(tip, new Text(dataArray[0]));
    }
}
