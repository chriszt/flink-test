package com.yl.flink.streaming.partition;

import com.yl.flink.streaming.beans.Trade;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class PartitionSource extends RichSourceFunction<Trade> {

    @Override
    public void run(SourceContext<Trade> ctx) throws Exception {
        System.out.println("[Source] " + getRuntimeContext().getTaskNameWithSubtasks());
        ctx.collect(new Trade("185xxxx", 899, "2018"));
        ctx.collect(new Trade("155xxxx", 1111, "2019"));
        ctx.collect(new Trade("155xxxx", 1199, "2019"));
        ctx.collect(new Trade("185xxxx", 899, "2018"));
        ctx.collect(new Trade("138xxxx", 19, "2019"));
        ctx.collect(new Trade("138xxxx", 399, "2020"));
    }

    @Override
    public void cancel() {

    }
}
