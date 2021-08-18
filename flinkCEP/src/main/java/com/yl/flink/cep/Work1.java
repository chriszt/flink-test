package com.yl.flink.cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Work1 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 流处理
        DataStream<LoginEvent> loginEventStream = env.fromCollection(Arrays.asList(
                new LoginEvent("小明","192.168.0.1","fail"),
                new LoginEvent("小明","192.168.0.2","fail"),
                new LoginEvent("小王","192.168.10,11","fail"),
                new LoginEvent("小王","192.168.10,12","fail"),
                new LoginEvent("小明","192.168.0.3","fail"),
                new LoginEvent("小明","192.168.0.4","fail"),
                new LoginEvent("小王","192.168.10,10","success"),
                new LoginEvent("小王","192.168.10,7","fail")

        ));


        Pattern<LoginEvent, LoginEvent> loginFailPattern = Pattern.<LoginEvent>
                begin("first")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent, Context<LoginEvent> context) throws Exception {
                        // System.out.println("first: " + loginEvent);
                        return loginEvent.getType().equals("fail");
                    }
                })
                .next("second")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent, Context<LoginEvent> context) throws Exception {
                        // System.out.println("second: " + loginEvent);
                        return loginEvent.getType().equals("fail");
                    }
                })
                .next("three")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent, Context<LoginEvent> context) throws Exception {
                        // System.out.println("three: " + loginEvent);
                        return loginEvent.getType().equals("fail");
                    }
                })
                .within(Time.seconds(2));
        // keyBy对同一个UserId进行分组，对同一个UserId进行匹配。
        PatternStream<LoginEvent> patternStream = CEP.pattern(
                loginEventStream.keyBy(LoginEvent::getUserId),
                loginFailPattern);

        DataStream<String> loginFailDataStream = patternStream.select(
                new PatternSelectFunction<LoginEvent, String>() {
                    @Override
                    public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
                        List<LoginEvent> second = pattern.get("three");
                        return  second.get(0).getUserId() + ", "+ second.get(0).getIp() + ", "+ second.get(0).getType();
                    }
                }
        );

        loginFailDataStream.print();
        env.execute();
    }

}
