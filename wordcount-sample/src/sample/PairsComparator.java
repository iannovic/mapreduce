package sample;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PairsComparator extends WritableComparator {
    protected  PairsComparator() {
        super(Text.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        Text key1 = (Text) w1;
        Text key2 = (Text) w2;
        String s1 = key1.toString();
        String s2 = key2.toString();
        String tokens1[] = s1.split("[-]");
        String tokens2[] = s2.split("[-]");
        
        if ((tokens1[0].equals(tokens2[0]))) {
        	if (s1.endsWith("-*") && !s2.endsWith("-*")) {
    			return -1;
    		} else if (!s1.endsWith("-*") && s2.endsWith("-*")){ 
    			return 1;
    		}
        }
        return key1.compareTo(key2);
    }
}

