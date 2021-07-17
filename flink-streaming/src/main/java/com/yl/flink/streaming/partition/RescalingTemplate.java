package com.yl.flink.streaming.partition;

import com.yl.flink.streaming.beans.Trade;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class RescalingTemplate {

    public static void main(String[] args) throws Exception {
        List<Trade> lst = new ArrayList<>();
        lst.add(new Trade("185XXX", 899, "2018"));
        lst.add(new Trade("155XXX", 1111, "2019"));
        lst.add(new Trade("155XXX", 1199, "2019"));
        lst.add(new Trade("185XXX", 899, "2018"));
        lst.add(new Trade("138XXX", 19, "2019"));
        lst.add(new Trade("138XXX", 399, "2020"));
        lst.add(new Trade("138XXX", 399, "2020"));
        lst.add(new Trade("138XXX", 399, "2020"));
        lst.add(new Trade("185XXX", 891, "2019"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<Trade> ds = env.fromCollection(lst);
//        ds.print("ds");
//        System.out.println("**************");
//        System.out.println("ds.parallelism=" + ds.getParallelism());
//        System.out.println("**************");

        DataStream<Trade> map1 = ds.partitionCustom(new Partitioner<String>() {
            @Override
            public int partition(String key, int numPartitions) {
                if(key.contains("185")) {
                    return 0;
                }
                return 1;
            }
        }, "cardNum");
//        System.out.println("**************");
//        System.out.println("map1.parallelism=" + map1.getParallelism());
//        System.out.println("**************");
//        map1.print("map1");

        DataStream<Trade> map2 = map1.map(new RichMapFunction<Trade, Trade>() {
            @Override
            public Trade map(Trade value) throws Exception {
                RuntimeContext rc = getRuntimeContext();
                String subTaskName = rc.getTaskNameWithSubtasks();
                int idx = rc.getIndexOfThisSubtask();
                System.out.println("[Map2] Trade: " + value + ", subTaskName: " + subTaskName + ", idx: " + idx);
                return value;
            }
        }).setParallelism(2);
//        map2.print("map2");

        DataStream<Trade> map3 = map2.rescale();

        DataStream<Trade> map4 = map3.map(new RichMapFunction<Trade, Trade>() {
            @Override
            public Trade map(Trade value) throws Exception {
                RuntimeContext rc = getRuntimeContext();
                String subTaskName = rc.getTaskNameWithSubtasks();
                int idx = rc.getIndexOfThisSubtask();
                System.out.println("[Map4] Trade: " + value + ", subTaskName: " + subTaskName + ", idx: " + idx);
                return value;
            }
        });
        map4.print();

        env.execute();
    }

}
