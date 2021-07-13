package com.yl.flink.streaming.chain;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.xml.crypto.Data;

public class NewChainTemplate {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStream<Tuple2<String, Integer>> ds = env.addSource(new ChainSource());
//        ds.print();

        DataStream<Tuple2<String, Integer>> flt = ds.filter(new RichFilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> value) throws Exception {
                System.out.println("[Filter] [TaskName] " + getRuntimeContext().getTaskNameWithSubtasks() + ", [elem] " + value);
                return true;
            }
        });
//        flt.print();

        DataStream<Tuple2<String, Integer>> map1 = flt.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                System.out.println("[Map1] [TaskName] " + getRuntimeContext().getTaskNameWithSubtasks() + ", [elem] " + value);
                return value;
            }
        });
//        map1.print("map1");

        map1 = ((SingleOutputStreamOperator<Tuple2<String, Integer>>) map1).startNewChain();

        DataStream<Tuple2<String, Integer>> map2 = map1.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                System.out.println("[Map2] [TaskName] " + getRuntimeContext().getTaskNameWithSubtasks() + ", [elem] " + value);
                return value;
            }
        });
        map2.print("map2");

        env.execute();
    }

}
