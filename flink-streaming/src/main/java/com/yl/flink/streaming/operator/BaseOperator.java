package com.yl.flink.streaming.operator;

import com.yl.flink.streaming.beans.Trade;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

import java.util.ArrayList;
import java.util.List;

public class BaseOperator {

    public void readTextFile(String filePath) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dss = env.readTextFile(filePath);
        dss.print();
        env.execute();
    }

    public void readFile(String filePath) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TextInputFormat fmt = new TextInputFormat(new Path(filePath));
        TypeInformation<String> typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
        DataStreamSource<String> dds = env.readFile(fmt, filePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 10000, typeInfo);
        dds.print();
        env.execute();
    }

    public void writeToScreen() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        DataStreamSource<Long> dds = env.fromElements(1L, 21L, 22L);
        dds.print("aaa");
        env.execute();
    }

    public void mapTemplate() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> dds1 = env.fromSequence(1, 5);
        DataStream<Tuple2<Long, Integer>> dds2 = dds1.map(new MapFunction<Long, Tuple2<Long, Integer>>() {
            @Override
            public Tuple2<Long, Integer> map(Long values) throws Exception {
                return new Tuple2<Long, Integer>(values * 100, values.hashCode());
            }
        });
        dds2.print();
        env.execute();
    }

    public void filterTemplate() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Long> ds1 = env.fromSequence(1L, 5L);
        DataStream<Long> ds2 = ds1.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                System.out.println(value);
                if (value == 2L || value == 4L) {
                    return false;
                }
                return true;
            }
        });
        ds2.print();
        env.execute();
    }

    public void keyByTemplate() throws Exception {
        List<Tuple2<Integer, Integer>> lst = new ArrayList<Tuple2<Integer, Integer>>();
        lst.add(new Tuple2<>(1, 11));
        lst.add(new Tuple2<>(1, 22));
        lst.add(new Tuple2<>(3, 33));
        lst.add(new Tuple2<>(5, 55));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<Integer, Integer>> ds1 = env.fromCollection(lst);
//        KeyedStream<Tuple2<Integer, Integer>, Tuple> ks1 = ds1.keyBy(0);
        KeyedStream<Tuple2<Integer, Integer>, Object> ks1 = ds1.keyBy(new KeySelector<Tuple2<Integer, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<Integer, Integer> value) throws Exception {
                return value.f0;
            }
        });
        ks1.print();
        env.execute();
    }

    public void reduceTemplate() throws Exception {
        List<Trade> lst = new ArrayList<>();
        lst.add(new Trade("123xxxx", 899, "2018-06"));
        lst.add(new Trade("123xxxx", 699, "2018-06"));
        lst.add(new Trade("188xxxx", 88, "2018-07"));
        lst.add(new Trade("188xxxx", 69, "2018-07"));
        lst.add(new Trade("158xxxx", 100, "2018-06"));
        lst.add(new Trade("158xxxx", 1000, "2018-06"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Trade> ds1 = env.fromCollection(lst);
        DataStream<Trade> ds2 = ds1.keyBy("cardNum").reduce(new ReduceFunction<Trade>() {
            @Override
            public Trade reduce(Trade value1, Trade value2) throws Exception {
                System.out.println(value1+"  "+value2);
                return new Trade(value1.getCardNum(), value1.getTrade() + value2.getTrade(), "----");
            }
        });
        ds2.print();
        env.execute();
    }
    public void aggreTemplate() throws Exception {
        List<Trade> lst = new ArrayList<>();
        lst.add(new Trade("188xxxx", 30, "2018-07"));
        lst.add(new Trade("188xxxx", 20, "2018-11"));
        lst.add(new Trade("158xxxx", 1, "2018-07"));
        lst.add(new Trade("158xxxx", 2, "2018-06"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Trade> ds = env.fromCollection(lst);
        KeyedStream<Trade, Tuple> ks = ds.keyBy("cardNum");
//        ks.sum("trade").print("sum");
        ks.min("trade").print("min");
//        ks.minBy("trade").print("minBy");
        env.execute();
    }

    @Deprecated
    public void splitTemplate() throws Exception {
        List<Trade> lst = new ArrayList<>();
        lst.add(new Trade("185xxxx", 899, "周一"));
        lst.add(new Trade("155xxxx", 1199, "周二"));
        lst.add(new Trade("138xxxx", 19, "周三"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Trade> ds = env.fromCollection(lst);
        env.execute();
    }

    public void projectTemplate() throws Exception {
        List<Tuple3<String, Integer, String>> lst = new ArrayList<>();
        lst.add(new Tuple3<>("185xxxx", 899, "周一"));
        lst.add(new Tuple3<>("155xxxx", 1199, "周二"));
        lst.add(new Tuple3<>("138xxxx", 19, "周三"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple3<String, Integer, String>> ds1 = env.fromCollection(lst);
        DataStream<Tuple2<String, String>> ds2 = ds1.<Tuple2<String, String>>project(2, 0);
        ds2.print();
        env.execute();
    }

    public void unionTemplate() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Long> ds1 = env.fromSequence(1, 2);
        DataStream<Long> ds2 = env.fromSequence(1001, 1002);
        DataStream<Long> ds3 = ds1.union(ds2);
        ds3.print();
        env.execute();
    }


}
