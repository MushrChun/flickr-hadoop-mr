package assign1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by MushrChun on 19/4/17.
 * input: [place-id,order], “1”
 */
public class Task1MergeReducer extends Reducer <TextIntPair,Text,Text,Text>{

    private Text locality = new Text();
    private Text sum = new Text();

    @Override
    protected void reduce(TextIntPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> valuesItr = values.iterator();

        // drop those that doesn't match data from photo mapper
        if(key.getOrder().get()!=0){
            return;
        }


        locality.set(valuesItr.next());

        int count = 0;
        while(valuesItr.hasNext()){
            count++;
            valuesItr.next();
        }

        if(count>0){
            sum.set(String.valueOf(count));
            context.write(locality, sum);
        }

    }
}
