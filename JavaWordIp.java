package com.lehoolive.spark.utils;


import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.ArrayList;
import java.util.List;


public class JavaWordIp {

    private static volatile Broadcast<List<String[]>> instance = null;

    public static Broadcast<List<String[]>> getInstance(JavaSparkContext jsc) {
        if (instance == null) {
            synchronized (JavaWordIp.class) {
                if (instance == null) {
                    List<String> ipCollect = jsc.textFile("hdfs://master:8020/user/ip.txt").collect();
                    List<String[]> ipList = new ArrayList<>();
                    try {
                        for (String line : ipCollect) {
                            if (!"".equals(line)) {
                                String[] split = line.split("\\|");
                                if (split[2] != null && split[3] != null && split[7] != null) {
                                    String[] ipSection = new String[3];
                                    ipSection[0] = split[2];
                                    ipSection[1] = split[3];
                                    ipSection[2] = split[7];
                                    ipList.add(ipSection);
                                }

                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    instance = jsc.broadcast(ipList);
                }
            }
        }
        return instance;
    }
}
