package com.yl.flink.table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MyTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

        DataStream<Long> ds = env.fromSequence(1, 5);
        ds.print("ds");




        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
