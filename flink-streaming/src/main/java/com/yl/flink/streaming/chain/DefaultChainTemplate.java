package com.yl.flink.streaming.chain;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DefaultChainTemplate {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStream<Tuple2<String, Integer>> ds = env.addSource(new ChainSource()).name("MyDataSource");
        DataStream<Tuple2<String, Integer>> flt = ds.filter(new RichFilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> value) throws Exception {
                System.out.println("[Filter] [TaskName] " + getRuntimeContext().getTaskNameWithSubtasks() + ", [elem] " + value);
                return true;
            }
        }).name("MyFilter");
        DataStream<Tuple2<String, Integer>> map1 = flt.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                System.out.println("[Map1] [TaskName] " + getRuntimeContext().getTaskNameWithSubtasks() + ", [elem] " + value);
                return value;
            }
        }).name("MyMap1");
        DataStream<Tuple2<String, Integer>> map2 = map1.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                System.out.println("[Map2] [TaskName] " + getRuntimeContext().getTaskNameWithSubtasks() + ", [elem] " + value);
                return value;
            }
        }).name("MyMap2");
        map2.print();

        env.execute();
    }

}
