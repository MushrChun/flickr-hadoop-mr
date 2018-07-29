package assign1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by MushrChun on 19/4/17.
 * Input:  photo-id,owner,tags,date-taken,place-id,accuracy
 */
public class Task1PhotoMergeMapper extends Mapper<Object, Text, TextIntPair, Text> {

    private TextIntPair tip = new TextIntPair();

    private Text One = new Text("1");

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] dataArray = value.toString().split("\t");

        // drop incomplete the record
        if (dataArray.length < 6){
            return;
        }

        tip.setKey(new Text(dataArray[4]));
        tip.setOrder(new IntWritable(1));

        context.write(tip , One);
    }
}
