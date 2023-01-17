package com.itcast.kafka;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.FastDateFormat;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DateTransformTest {
    public static void main(String[] args) {
        String filePath1 = "D:\\project\\test-kafka-api\\data\\test.csv";
        String filePath2 = "D:\\project\\test-kafka-api\\data\\Taxi_Trips2.csv";
        String filePath3 = "D:\\project\\test-kafka-api\\data\\Taxi_Trips3.csv";
        String filePath4 = "D:\\project\\test-kafka-api\\data\\Taxi_Trips4.csv";
        String filePath5 = "D:\\project\\test-kafka-api\\data\\Taxi_Trips5.csv";
        String filePath6 = "D:\\project\\test-kafka-api\\data\\Taxi_Trips6.csv";
        ExecutorService executorService = Executors.newFixedThreadPool(6);
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                write2File(filePath1);
            }
        });
        /*executorService.submit(new Runnable() {
            @Override
            public void run() {
                write2File(filePath2);
            }
        });
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                write2File(filePath3);
            }
        });
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                write2File(filePath4);
            }
        });
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                write2File(filePath5);
            }
        });
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                write2File(filePath6);
            }
        });*/

    }

    public static void write2File(String filePath){
        String text = null;
        try {
            text = FileUtils.readFileToString(new File(filePath));
        } catch (IOException e) {
            e.printStackTrace();
        }
        String formatDate = getFormatDate(text);
        try {
            FileUtils.write(new File("D:\\project\\test-kafka-api\\data\\Taxi_Trips_f.csv"),formatDate,true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getFormatDate(String text){
        String pattern = "(0[1-9]|1[0-2])/(0[1-9]|[1-2][0-9]|3[0-1])/[1-9]\\d{3}\\s+(20|21|22|23|[0-1]\\d):[0-5]\\d:[0-5]\\d\\s+(PM|AM)";
        //1. 模式匹配
        String result = text;
        Matcher matcher = Pattern.compile(pattern).matcher(text);
        while(matcher.find()){
            System.out.println(matcher.group());
            System.out.println(getTime(matcher.group()));
            result = result.replaceAll(matcher.group(), getTime(matcher.group()));
        }
        return result;
    }

    private static String getTime(String patternTime){
        FastDateFormat instance = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
        String date = instance.format(new Date(patternTime));
        return date;
    }
}
