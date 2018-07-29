package assign1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by MushrChun on 20/4/17.
 * This mapper is for Task3 Job2 results which in form of locality, numofphotos, tags, date-taken
 */
public class Task3Job3Mapper extends Mapper<Object, Text, Text, IntWritable>{

    private IntWritable one = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] dataArray = value.toString().split("\t");

        String locality = dataArray[0];
        String numOfPhotos = dataArray[1];
        String tags = dataArray[2];
        String dateTaken = dataArray[3];
        String[] tagArr = tags.split(" ");
        for(String tag: tagArr){
            //omit location
            String cleanStr = locality.toLowerCase().replaceAll("\\+","");
            if(cleanStr.indexOf(tag.toLowerCase())>=0){
                continue;
            }
            // omit year
            if(dateTaken.indexOf(tag)>=0){
                continue;
            }
            context.write(new Text(locality+"\t"+numOfPhotos+"\t"+tag), one);
        }

    }
}
