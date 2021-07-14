package com.yl.flink.streaming.chain;

import com.yl.flink.streaming.beans.Trade;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.ArrayList;
import java.util.List;

public class MyDataSource extends RichSourceFunction<Trade> {

    @Override
    public void run(SourceContext<Trade> ctx) throws Exception {
        Trade t1 = new Trade("no-1", 11, "2021");
        System.out.println("[MyDataSource] " + getRuntimeContext().getTaskNameWithSubtasks() + ", [elem] " + t1);
        ctx.collect(t1);

        Thread.sleep(1000);

        Trade t2 = new Trade("no-2", 22, "2022");
        System.out.println("[MyDataSource] " + getRuntimeContext().getTaskNameWithSubtasks() + ", [elem] " + t2);
        ctx.collect(t2);
    }

    @Override
    public void cancel() {

    }
}
