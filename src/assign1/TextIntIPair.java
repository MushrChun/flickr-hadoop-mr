package assign1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextIntIPair implements WritableComparable<TextIntIPair> {

    private Text key;
    private IntWritable order;

    public TextIntIPair() {
        this.key = new Text();
        this.order = new IntWritable();
    }

    public TextIntIPair(String key, int order) {
        this.key = new Text(key);
        this.order = new IntWritable(order);
    }

    public Text getKey() {
        return key;
    }

    public void setKey(Text key) {
        this.key = key;
    }

    public IntWritable getOrder() {
        return order;
    }

    public void setOrder(IntWritable order) {
        this.order = order;
    }

    public void readFields(DataInput in) throws IOException {
        key.readFields(in);
        order.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        key.write(out);
        order.write(out);
    }

    @Override
    public int compareTo(TextIntIPair other) {
        int cmp = key.compareTo(other.key);
        if (cmp != 0) {
            return cmp;
        }
        return -order.compareTo(other.order);
    }

    @Override
    public int hashCode() {
        return key.hashCode() * 163 - order.get();
    }

    public boolean equals(Object other) {
        if (other instanceof TextIntIPair) {
            TextIntIPair tip = (TextIntIPair) other;
            return key.equals(tip.key) && order.equals(tip.order);
        }
        return false;
    }
}
