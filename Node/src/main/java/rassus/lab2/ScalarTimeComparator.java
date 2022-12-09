package rassus.lab2;

import java.util.Comparator;

public class ScalarTimeComparator implements Comparator<Reading> {
    @Override
    public int compare(Reading o1, Reading o2) {
        Long scalarTime1 = o1.getScalarTime();
        Long scalarTime2 = o2.getScalarTime();
        return scalarTime1.compareTo(scalarTime2);
    }
}
