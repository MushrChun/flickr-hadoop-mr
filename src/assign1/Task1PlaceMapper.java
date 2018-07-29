package assign1;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by MushrChun on 20/4/17.
 * This mapper is for photos: place-id, woeid, latitudelongitude, place-name, place-type-id, place-url
 */
public class Task1PlaceMapper extends Mapper<Object, Text, TextIntPair, Text> {


    private Text placeId = new Text();


    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] dataArray = value.toString().split("\t");

        // drop incomplete the record
        if (dataArray.length < 7){
            return;
        }

        // drop non 7 and 22 place-type
        if(dataArray[5].trim().equals("7")||dataArray[5].trim().equals("22")){
            // [6] is place-url [4] is place-name
            placeId.set(dataArray[0]);
            context.write(new TextIntPair(dataArray[6], Integer.parseInt(dataArray[5])), placeId);
        }
    }
}