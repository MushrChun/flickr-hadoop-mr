package assign1;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TextIntPartitioner extends Partitioner<TextIntPair,Text> {

	@Override
	public int getPartition(TextIntPair key, Text value, int numPartition) {
		return (key.getKey().hashCode() & Integer.MAX_VALUE) % numPartition;
	}
		
}
