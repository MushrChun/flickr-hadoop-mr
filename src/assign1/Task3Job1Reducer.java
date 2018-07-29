package assign1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by MushrChun on 20/4/17.
 */
public class Task3Job1Reducer extends Reducer<TextIntPair,Text, Text, Text>{
    @Override
    protected void reduce(TextIntPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        // drop data which dont own numofphotos info
        if(key.getOrder().get()!=0){
            return;
        }


        Iterator<Text> valueItr = values.iterator();

        Text numofPhotos = new Text(valueItr.next().toString());

        while(valueItr.hasNext()){
            context.write(new Text(key.getKey().toString()+"\t"+numofPhotos.toString()), valueItr.next());
        }
    }
}
