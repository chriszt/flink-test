package com.yl.flink.streaming.partition;

import com.yl.flink.streaming.beans.Trade;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.xml.crypto.Data;

public class ShuffleTemplate {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStream<Trade> ds = env.addSource(new PartitionSource());
//        ds.print("ds");

        DataStream<Trade> map1 = ds.map(new RichMapFunction<Trade, Trade>() {
            @Override
            public Trade map(Trade value) throws Exception {
                RuntimeContext rc = getRuntimeContext();
                String subTaskName = rc.getTaskNameWithSubtasks();
                int idx = rc.getIndexOfThisSubtask();
                System.out.println("[Map1] Trade: " + value + ", subTaskName: " + subTaskName + ", idx: " + idx);
                return value;
            }
        });
//        map1.print("map1");

        DataStream<Trade> map2 = map1.shuffle();

        DataStream<Trade> map3 = map2.map(new RichMapFunction<Trade, Trade>() {
            @Override
            public Trade map(Trade value) throws Exception {
                RuntimeContext rc = getRuntimeContext();
                String subTaskName = rc.getTaskNameWithSubtasks();
                int idx = rc.getIndexOfThisSubtask();
                System.out.println("[Map3] Trade: " + value + ", subTaskName: " + subTaskName + ", idx: " + idx);
                return value;
            }
        });
        map3.print("map3");

        env.execute();
    }

}
