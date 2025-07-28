package org.example.datastream.flink.paimon;

import org.example.util.MyParameter;

public class Main {
    public static void main(String[] args) throws Exception {
        String parameter = MyParameter.getParameter("dev","host");
        System.out.println(parameter);
        DataGeneratorWriteToMinIO.run();
    }
}
