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
import java.util.List;
import java.util.Map;

public class Test1 {

    public static void main(String[] args) {
        List<LoginEvent> lst = new ArrayList<>();
        lst.add(new LoginEvent("小明","192.168.0.1","fail"));
        lst.add(new LoginEvent("小明","192.168.0.2","fail"));
        lst.add(new LoginEvent("小明","192.168.10.11","fail"));
        lst.add(new LoginEvent("小明","192.168.10.12","fail"));
        lst.add(new LoginEvent("小明","192.168.0.3","fail"));
        lst.add(new LoginEvent("小明","192.168.0.4","fail"));
        lst.add(new LoginEvent("小明","192.168.10.10","success"));
        lst.add(new LoginEvent("小明","192.168.10.7","fail"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<LoginEvent> loginStream = env.fromCollection(lst);
        loginStream.print("loginStream");

        Pattern<LoginEvent, ?> loginFailPattern = Pattern.<LoginEvent>begin("first").where(new IterativeCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent event, Context<LoginEvent> ctx) throws Exception {
//                System.out.println(event);
                return event.getType().equals("fail");
            }
        }).next("second").where(new IterativeCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent event, Context<LoginEvent> ctx) throws Exception {
                return event.getType().equals("fail");
            }
        }).next("three").where(new IterativeCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent event, Context<LoginEvent> ctx) throws Exception {
                return event.getType().equals("fail");
            }
        }).within(Time.seconds(10));

        PatternStream<LoginEvent> patternStream = CEP.pattern(loginStream.keyBy(LoginEvent::getUserId), loginFailPattern);
//        PatternStream<LoginEvent> patternStream = CEP.pattern(loginStream, loginFailPattern);

        DataStream<String> loginFailStream = patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
//                System.out.println("***********");
//                List<LoginEvent> second = pattern.get("three");
//                return second.get(0).getUserId() + ", " + second.get(0).getIp() + ", " + second.get(0).getType();
                return "****";
            }
        });

        loginFailStream.print("loginFailStream");

        try {
            env.execute();
        } catch (Exception e) {
            System.err.println("Execution Fail.");
        }
    }

}
