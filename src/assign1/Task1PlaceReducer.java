package assign1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by MushrChun on 20/4/17.
 * input: [place-name, place-type-id], place-id
 */
public class Task1PlaceReducer extends  Reducer<TextIntPair, Text, Text, Text> {

	private Text locality = new Text();

	public void reduce(TextIntPair key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		Iterator<Text> valuesItr = values.iterator();

		if (key.getOrder().get() != 7){
			String[] splits = key.getKey().toString().split("/");
			StringBuilder sb = new StringBuilder();
			for(int i =1 ; i< splits.length-1; i++){
				sb.append("/");
				sb.append(splits[i]);
			}

			locality.set(sb.toString());
			while(valuesItr.hasNext()){
				context.write(locality, valuesItr.next());
			}
		}else{
			locality.set(key.getKey());
			while(valuesItr.hasNext()){
				context.write(locality, valuesItr.next());
			}
		}
	}
}
