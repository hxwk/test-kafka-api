package com.itcast.kafka;

import java.util.Arrays;

public class SplitTest {
    public static void main(String[] args) {
        String bigdatas = "hadoop#CS#spark#CS#flink#CS#zookeeper#CS#kafka#CS#elk#CS#mysql";
        String[] bigData = bigdatas.split("#CS#", -1);
        Arrays.asList(bigData).forEach(x->System.out.println(x));

        System.out.println("-----------------------------------");
        String[] bigData2 = bigdatas.split("#CS#", 3);
        Arrays.asList(bigData2).forEach(x->System.out.println(x));

        System.out.println("-----------------------------------");
        String[] bigData3 = bigdatas.split("#CS#");
        Arrays.asList(bigData3).forEach(x->System.out.println(x));

        System.out.println("-----------------------------------");
        String[] bigData4 = bigdatas.split("#CS#",0);
        Arrays.asList(bigData4).forEach(x->System.out.println(x));

        System.out.println("-----------------------------------");
        String[] bigData5 = bigdatas.split("#CS#",-2);
        Arrays.asList(bigData5).forEach(x->System.out.println(x));
    }
}
