package assign1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by MushrChun on 20/4/17.
 * This mapper is for Task3 Job3 results which in form of locality, numofphotos, tag, freq
 */
public class Task3Job4Mapper extends Mapper<Object, Text, TextIntIPair, Text>{

    private TextIntIPair tip = new TextIntIPair();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] dataArray = value.toString().split("\t");
        tip.setKey(new Text(dataArray[0]+"\t"+dataArray[1]));
        tip.setOrder(new IntWritable(Integer.parseInt(dataArray[3])));
        context.write(tip, new Text(dataArray[2]+":"+dataArray[3]));

    }
}
