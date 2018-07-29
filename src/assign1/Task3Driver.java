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
 * Created by MushrChun on 20/4/17.
 */
public class Task3Driver {
    public static void main (String args[]) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: Task3 Driver <task2 out> <photo in> <out>");
            System.exit(2);
        }

        Path task3Tmp = new Path("task3Tmp");
        Path task3Tmp2 = new Path("task3Tmp2");
        Path task3Tmp3 = new Path("task3Tmp3");
        Path task3Tmp4 = new Path("task3Tmp4");
        FileSystem.get(conf).delete(new Path(otherArgs[2]), true);
        FileSystem.get(conf).delete(task3Tmp, true);
        FileSystem.get(conf).delete(task3Tmp2, true);
        FileSystem.get(conf).delete(task3Tmp3, true);
        FileSystem.get(conf).delete(task3Tmp4, true);

        // Job1 start here
        Job job1 = new Job(conf, "Comp5349 Assign1 Task3 Job1 - Chun Shen");
        job1.setJarByClass(Task3Driver.class);

        MultipleInputs.addInputPath(job1, new Path(otherArgs[0]), TextInputFormat.class, Task3Job1Mapper1.class);
        MultipleInputs.addInputPath(job1, new Path("task1Tmp"), TextInputFormat.class, Task3Job1Mapper2.class);

        job1.setMapOutputKeyClass(TextIntPair.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setGroupingComparatorClass(TextIntPairGroupComparator.class);

        job1.setNumReduceTasks(1);
        job1.setReducerClass(Task3Job1Reducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        TextOutputFormat.setOutputPath(job1, task3Tmp);

        job1.waitForCompletion(true);


        // Job2 start here
        Job job2 = new Job(conf, "Comp5349 Assign1 Task3 Job2 - Chun Shen");
        job2.setJarByClass(Task3Driver.class);

        MultipleInputs.addInputPath(job2, task3Tmp, TextInputFormat.class, Task3Job2Mapper1.class);
        MultipleInputs.addInputPath(job2, new Path(otherArgs[1]), TextInputFormat.class, Task3Job2Mapper2.class);

        job2.setMapOutputKeyClass(TextIntPair.class);
        job2.setMapOutputValueClass(Text.class);


        job2.setGroupingComparatorClass(TextIntPairGroupComparator.class);
        job2.setPartitionerClass(TextIntPartitioner.class);

        job2.setNumReduceTasks(5);
        job2.setReducerClass(Task3Job2Reducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        TextOutputFormat.setOutputPath(job2, task3Tmp2);

        job2.waitForCompletion(true);


        // Job3 start here
        Job job3 = new Job(conf, "Comp5349 Assign1 Task3 Job3 - Chun Shen");
        job3.setJarByClass(Task3Driver.class);

        MultipleInputs.addInputPath(job3, task3Tmp2, TextInputFormat.class, Task3Job3Mapper.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(IntWritable.class);

        job3.setNumReduceTasks(1);
        job3.setCombinerClass(Task3Job3Combiner.class);
        job3.setReducerClass(Task3Job3Reducer.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        TextOutputFormat.setOutputPath(job3, task3Tmp3);

        job3.waitForCompletion(true);


        // Job4 start here
        Job job4 = new Job(conf, "Comp5349 Assign1 Task3 Job4 - Chun Shen");
        job4.setJarByClass(Task3Driver.class);

        MultipleInputs.addInputPath(job4, task3Tmp3, TextInputFormat.class, Task3Job4Mapper.class);

        job4.setMapOutputKeyClass(TextIntIPair.class);
        job4.setMapOutputValueClass(Text.class);

        job4.setGroupingComparatorClass(TextIntPairGroupComparator.class);

        job4.setNumReduceTasks(1);
        job4.setReducerClass(Task3Job4Reducer.class);

        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);

        TextOutputFormat.setOutputPath(job4, task3Tmp4);

        job4.waitForCompletion(true);

        // Job5 start here
        Job job5 = new Job(conf, "Comp5349 Assign1 Task3 Jo5 - Chun Shen");
        job5.setJarByClass(Task3Driver.class);

        MultipleInputs.addInputPath(job5, task3Tmp4, TextInputFormat.class, Task3Job5Mapper.class);

        job5.setMapOutputKeyClass(IntWritable.class);
        job5.setMapOutputValueClass(Text.class);

        job5.setSortComparatorClass(IntWritableDescendingComparator.class);

        job5.setNumReduceTasks(1);
        job5.setReducerClass(Task3Job5Reducer.class);

        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);

        TextOutputFormat.setOutputPath(job5, new Path(otherArgs[2]));

        job5.waitForCompletion(true);
    }
}
