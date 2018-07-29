package assign1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by cshe6391 on 4/19/2017.
 * input: numofphotos, place-name
 * output: place-name, numofphotos
 */
public class Task2Job1Reducer extends Reducer<IntWritable, Text, Text, Text>{

    private int top = 50;
    private Text outputKey = new Text();
    private Text outputValue = new Text();

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        while(context.nextKey() && top>0){
            reduce(context.getCurrentKey(), context.getValues(), context);
        }
        cleanup(context);
    }

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> valuesItr = values.iterator();
        outputValue.set(String.valueOf(key));
        while(valuesItr.hasNext()){
            if(top<=0)break;
            outputKey.set(valuesItr.next().toString());
            context.write(outputKey, outputValue );
            top--;
        }
    }
}
