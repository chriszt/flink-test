package com.yl.flink.streaming.chain;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MyChain {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Long> ds = env.fromSequence(1, 5);
        ds.print("ds");

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
