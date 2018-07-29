package assign1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Created by cshe6391 on 4/19/2017.
 */
public class Task2Driver {

    public static void main (String args[]) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Task2 Driver <task1 result folder> <out>");
            System.exit(2);
        }

        // Clean output path
        FileSystem.get(conf).delete(new Path(otherArgs[1]), true);

        Job job1 = new Job(conf, "Comp5349 Assign1 Task2 Job1 - Chun Shen");
        job1.setJarByClass(Task2Driver.class);

        MultipleInputs.addInputPath(job1, new Path(otherArgs[0]), TextInputFormat.class, Task2Job1Mapper.class);

        job1.setSortComparatorClass(IntWritableDescendingComparator.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setNumReduceTasks(1);
        job1.setReducerClass(Task2Job1Reducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        TextOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));

        job1.waitForCompletion(true);

    }
}
