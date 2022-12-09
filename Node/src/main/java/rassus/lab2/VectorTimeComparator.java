package rassus.lab2;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class VectorTimeComparator implements Comparator<Reading> {

    @Override
    public int compare(Reading o1, Reading o2) {
        int count1 = 0;
        int count2 = 0;

        HashMap<String, Integer> vectorTime1 = o1.getVectorTime();
        HashMap<String, Integer> vectorTime2 = o2.getVectorTime();

        for(Map.Entry<String, Integer> e1 : vectorTime1.entrySet()) {
            for(Map.Entry<String, Integer> e2 : vectorTime2.entrySet()) {
                if(e1.getKey().equalsIgnoreCase(e2.getKey())) {
                    if(e1.getValue() > e2.getValue()) {
                        count1++;
                    }
                    else if (e1.getValue() < e2.getValue()) {
                        count2++;
                    }
                }
            }
        }

        if(count1 == 0 && count2 != 0) {
            return -1;
        }
        else if (count2 == 0 && count1 != 0) {
            return 1;
        }
        else {
            return 0;
        }
    }
}
