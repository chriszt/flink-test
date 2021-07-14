package com.yl.flink.streaming.chain;

import com.yl.flink.streaming.beans.Trade;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

public class MyChain {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStream<Trade> ds = env.addSource(new MyDataSource());
//        ds.print("ds");

        DataStream<Trade> flt = ds.filter(new RichFilterFunction<Trade>() {
            @Override
            public boolean filter(Trade value) throws Exception {
                return true;
            }
        });
        flt.print("flt");

        env.execute();
    }

}
