package com.yl.flink.streaming.helloworld;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCount {

    public static final String[] WORDS = new String[] {
            "com.yl.flink.streaming.helloworld.WordCountTemplate",
            "com.yl.flink.streaming.helloworld.WordCountTemplate",
            "com.yl.flink.streaming.helloworld.WordCountTemplate",
            "com.yl.flink.streaming.helloworld.WordCountTemplate"
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.fromElements(WORDS);

        DataStream<Tuple2<String, Integer>> word = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] tokens = value.toLowerCase().split("\\.");
                for (String token : tokens) {
                    System.out.println(token);
                    if (token.length() > 0) {
                        out.collect(new Tuple2<>(token, 1));
                    }
                }
            }
        });

        DataStream<Tuple2<String, Integer>> counts = word.keyBy("f0").sum(1);
        counts.print("Hello Data Stream");
        env.execute();
    }

}
