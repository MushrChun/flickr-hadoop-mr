package assign1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Task1Driver {

    public static void main (String args[]) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: Task1Driver <place in> <photo in> <out> ");
            System.exit(2);
        }

        Path task1Tmp = new Path("task1Tmp");
        Path task1Tmp2 = new Path("task1Tmp2");

        // Clean previous temporary path if needed
        FileSystem.get(conf).delete(task1Tmp, true);
        FileSystem.get(conf).delete(task1Tmp2, true);
        // Clean output path
        FileSystem.get(conf).delete(new Path(otherArgs[2]), true);

        // Job1 start here
        Job job1 = new Job(conf, "Comp5349 Assign1 Task1 Job1 - Chun Shen");
        job1.setJarByClass(Task1Driver.class);

        MultipleInputs.addInputPath(job1, new Path(otherArgs[0]), TextInputFormat.class, Task1PlaceMapper.class);

        job1.setMapOutputKeyClass(TextIntPair.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setPartitionerClass(TextIntPartitioner.class);
        job1.setGroupingComparatorClass(Task1GroupComparator.class);

        job1.setNumReduceTasks(5);
        job1.setReducerClass(Task1PlaceReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        TextOutputFormat.setOutputPath(job1, task1Tmp);

        job1.waitForCompletion(true);

        // Job2 start here
        Job job2 = new Job(conf, "Comp5349 Assign1 Task1 Job2 - Chun Shen");
        job2.setJarByClass(Task1Driver.class);

        MultipleInputs.addInputPath(job2, task1Tmp, TextInputFormat.class, Task1PlaceMergeMapper.class);
        MultipleInputs.addInputPath(job2, new Path(otherArgs[1]), TextInputFormat.class, Task1PhotoMergeMapper.class);

        job2.setMapOutputKeyClass(TextIntPair.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setPartitionerClass(TextIntPartitioner.class);
        job2.setGroupingComparatorClass(TextIntPairGroupComparator.class);

        job2.setNumReduceTasks(5);
        job2.setReducerClass(Task1MergeReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        TextOutputFormat.setOutputPath(job2, task1Tmp2);

        job2.waitForCompletion(true);


        // Job3 start here
        Job job3 = new Job(conf, "Comp5349 Assign1 Task1 Job3 - Chun Shen");
        job3.setJarByClass(Task1Driver.class);

        MultipleInputs.addInputPath(job3, task1Tmp2, TextInputFormat.class, Task1SortMapper.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);

        job3.setNumReduceTasks(1);
        job3.setReducerClass(Task1SortReducer.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        TextOutputFormat.setOutputPath(job3, new Path(otherArgs[2]));

        job3.waitForCompletion(true);
    }
}
