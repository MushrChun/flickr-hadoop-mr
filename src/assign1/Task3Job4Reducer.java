package assign1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by MushrChun on 20/4/17.
 */
public class Task3Job4Reducer extends Reducer<TextIntIPair,Text, Text, Text>{
    @Override
    protected void reduce(TextIntIPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Iterator<Text> valueItr = values.iterator();

        StringBuilder sb = new StringBuilder();
        int count = 0;
        while(valueItr.hasNext()){
            if(count>=10){
                break;
            }
            String tag = valueItr.next().toString();
            sb.append("("+tag+") ");
            count++;
        }
        context.write(new Text(key.getKey()), new Text("["+sb.toString()+"]"));
    }
}
