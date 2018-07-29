package assign1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class Task1GroupComparator extends WritableComparator {

	protected Task1GroupComparator() {
		super(TextIntPair.class,true);
	}



	public int compare(WritableComparable w1, WritableComparable w2) {
		TextIntPair ltp1 = (TextIntPair) w1;
		TextIntPair ltp2 = (TextIntPair) w2;
		String ltp1locality = ltp1.getKey().toString();
		String ltp2locality = ltp2.getKey().toString();


		if(ltp1locality.indexOf(ltp2locality)>=0||
				ltp2locality.indexOf(ltp1locality)>=0){
			return 0;
		}


		return ltp1locality.compareTo(ltp2locality);
	}
	
}
