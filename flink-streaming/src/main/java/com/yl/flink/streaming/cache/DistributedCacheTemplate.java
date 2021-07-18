package com.yl.flink.streaming.cache;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DistributedCacheTemplate {

    public static void main(String[] args) {
        String filePath = "/home/yl/proj/flink-test/flink-streaming/src/main/resources/localFile.txt";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.registerCachedFile(filePath, "localFile.txt");

        DataStream<Long> ds = env.fromSequence(1, 4);
//        ds.print("ds");

        DataStream<String> map1 = ds.map(new MyMapper());
        map1.print("map1");

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
