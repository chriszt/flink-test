package com.yl.flink.streaming.chain;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;


public class ChainSource extends RichSourceFunction<Tuple2<String, Integer>> {

    final public static int sleep = 3000;

    @Override
    public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
        ctx.collect(new Tuple2<>("185xxxx", 899));
        System.out.println("[Source] [TaskName] "+ getRuntimeContext().getTaskNameWithSubtasks() +", [elem] " + new Tuple2<String, Integer>("185xxxx", 899));
        Thread.sleep(sleep);

        ctx.collect(new Tuple2<>("155xxxx", 1199));
        System.out.println("[Source] [TaskName] "+ getRuntimeContext().getTaskNameWithSubtasks() +", [elem] " + new Tuple2<String, Integer>("155xxxx", 1199));
        Thread.sleep(sleep);

        ctx.collect(new Tuple2<>("138xxxx", 19));
        System.out.println("[Source] [TaskName] "+ getRuntimeContext().getTaskNameWithSubtasks() +", [elem] " + new Tuple2<String, Integer>("138xxxx", 19));
        Thread.sleep(sleep);
    }

    @Override
    public void cancel() {

    }
}
