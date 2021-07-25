package com.yl.flink.streaming.window.process.windows;

import com.yl.flink.streaming.window.source.SourceForWindow;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class WindowsTemplate {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        DataStream<Tuple3<String, Integer, String>> ds = env.addSource(new SourceForWindow(1000, true));
//        ds.print("ds");

        KeyedStream<Tuple3<String, Integer, String>, String> key1 = ds.keyBy(new KeySelector<Tuple3<String, Integer, String>, String>() {
            @Override
            public String getKey(Tuple3<String, Integer, String> value) throws Exception {
                return value.f0;
            }
        });
//        key1.print("key1");

//        WindowedStream<Tuple3<String, Integer, String>, String, TimeWindow> win1 = key1.window(TumblingProcessingTimeWindows.of(Time.seconds(3)));
//        DataStream<Tuple3<String, Integer, String>> sum = win1.sum("f1");
//        sum.print("sum");

        WindowedStream<Tuple3<String, Integer, String>, String, TimeWindow> win2 = key1.window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(2)));
        DataStream<Tuple3<String, Integer, String>> sum = win2.sum("f1");
        sum.print("sum");

//        WindowedStream<Tuple3<String, Integer, String>, String, TimeWindow> win3 = key1.window(ProcessingTimeSessionWindows.withGap(Time.seconds(8)));
//        DataStream<Tuple3<String, Integer, String>> sum = win3.sum("f1");
//        sum.print("sum");

//        WindowedStream<Tuple3<String, Integer, String>, String, GlobalWindow> win4 = key1.window(GlobalWindows.create()).trigger(CountTrigger.of(3));
//        DataStream<Tuple3<String, Integer, String>> sum = win4.sum("f1");
//        sum.print("sum");

        try {
            JobExecutionResult ret = env.execute();
            System.out.println(ret.getNetRuntime());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
