package org.example.datastream.paimon.flink;

public class Main {
    public static void main(String[] args) {
        try {
            WriteToTable.writeTo();
            // ReadFromTable.readFrom();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
