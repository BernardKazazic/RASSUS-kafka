package rassus.lab2;

import java.util.HashMap;

public class Message {
    private String type;
    private long scalarTime;
    private HashMap<String, Integer> vectorTime;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public long getScalarTime() {
        return scalarTime;
    }

    public void setScalarTime(long scalarTime) {
        this.scalarTime = scalarTime;
    }

    public HashMap<String, Integer> getVectorTime() {
        return vectorTime;
    }

    public void setVectorTime(HashMap<String, Integer> vectorTime) {
        this.vectorTime = vectorTime;
    }
}
