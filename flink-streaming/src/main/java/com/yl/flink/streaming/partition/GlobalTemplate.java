package com.yl.flink.streaming.partition;

import com.yl.flink.streaming.beans.Trade;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class GlobalTemplate {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(3);

        DataStream<Trade> ds = env.addSource(new PartitionSource());
//        ds.print("ds");

        DataStream<Trade> map1 = ds.map(new RichMapFunction<Trade, Trade>() {
            @Override
            public Trade map(Trade value) throws Exception {
                RuntimeContext rc = getRuntimeContext();
                String subTaskName = rc.getTaskNameWithSubtasks();
                int subTaskIdxOf = rc.getIndexOfThisSubtask();
                System.out.println("元素值：" + value + " 分区策略前子任务名称：" + subTaskName + "，子任务编号：" + subTaskIdxOf);
                return value;
            }
        });
//        map1.print("map1");

        DataStream<Trade> map2 = map1.global();
//        map2.print("map2");

        DataStream<Trade> map3 = map2.map(new RichMapFunction<Trade, Trade>() {
            @Override
            public Trade map(Trade value) throws Exception {
                RuntimeContext rc = getRuntimeContext();
                String subTaskName = rc.getTaskNameWithSubtasks();
                int subTaskIdxOf = rc.getIndexOfThisSubtask();
                System.out.println("元素值：" + value + " 分区策略后子任务名称：" + subTaskName + "，子任务编号：" + subTaskIdxOf);
                return value;
            }
        });
        map3.print("map3");

        env.execute();
    }

}
