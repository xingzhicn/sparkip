package com.lehoolive.spark.utils;


import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.List;


public class JavaWordIp{

  private static volatile Broadcast<List<String[]>> instance = null;

  public static Broadcast<List<String[]>> getInstance(JavaSparkContext jsc,List<String[]> broadcast) {
    if (instance == null) {
      synchronized (JavaWordIp.class) {
        if (instance == null) {
          instance = jsc.broadcast(broadcast);
        }
      }
    }
    return instance;
  }
}