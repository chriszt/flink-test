package com.yl.flink.streaming.param;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ConstructorTemplate {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<Long> ds = env.fromSequence(1, 15);
//        ds.print("ds");

        ParamBean pb = new ParamBean("chriszt", 10);
        DataStream<Long> flt = ds.filter(new FilterConstructed(pb));
        flt.print("flt");

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
