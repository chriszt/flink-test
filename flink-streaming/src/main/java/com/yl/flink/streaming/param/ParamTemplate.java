package com.yl.flink.streaming.param;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ParamTemplate {

    public static void main(String[] args) throws Exception {
//        ParameterTool param4Jvm = ParameterTool.fromSystemProperties();
//        Properties props = param4Jvm.getProperties();
//        props.list(System.out);

//        String propPath = "/home/yl/proj/flink-test/flink-streaming/src/main/resources/flink-param.properties";
//        ParameterTool paramFromMain = ParameterTool.fromPropertiesFile(propPath);
//        Properties props = paramFromMain.getProperties();
//        props.list(System.out);

//        InputStream is = ParamTemplate.class.getClassLoader().getResourceAsStream("flink-param.properties");
//        ParameterTool paramFromMain = ParameterTool.fromPropertiesFile(is);
//        Properties props = paramFromMain.getProperties();
//        props.list(System.out);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration cfg = new Configuration();
        cfg.setLong("limit", 16);
        ExecutionConfig exeCfg = env.getConfig();
        exeCfg.setGlobalJobParameters(cfg);

        DataStream<Long> ds = env.fromSequence(1, 20);
        ds.filter(new FilterJobParameters()).print("aaa");
        env.execute();
    }

}
