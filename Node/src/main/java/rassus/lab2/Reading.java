package rassus.lab2;

import rassus.lab2.DTO.ReadingMessage;

import java.util.HashMap;

public class Reading {
    private long scalarTime;
    private HashMap<String, Integer> vectorTime;
    private double no2Reading;

    public Reading() {
        super();
    }

    public Reading(ReadingMessage message) {

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

    public double getNo2Reading() {
        return no2Reading;
    }

    public void setNo2Reading(double no2Reading) {
        this.no2Reading = no2Reading;
    }

    @Override
    public String toString() {
        return "Reading{" +
                "scalarTime=" + scalarTime +
                ", vectorTime=" + vectorTime +
                ", no2Reading=" + no2Reading +
                '}';
    }
}
