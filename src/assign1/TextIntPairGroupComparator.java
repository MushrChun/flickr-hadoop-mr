package assign1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by MushrChun on 19/4/17.
 */
public class TextIntPairGroupComparator extends WritableComparator {

    protected TextIntPairGroupComparator() {
        super(TextIntPair.class,true);
    }


    public int compare(WritableComparable w1, WritableComparable w2) {
        TextIntPair tip1 = (TextIntPair) w1;
        TextIntPair tip2 = (TextIntPair) w2;
        return tip1.getKey().compareTo(tip2.getKey());
    }
}
