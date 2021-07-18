package com.yl.flink.streaming.param;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;

public class FilterJobParameters extends RichFilterFunction<Long> {

    private long limit;

    @Override
    public void open(Configuration cfg) throws Exception {
        ExecutionConfig exeCfg = getRuntimeContext().getExecutionConfig();
        ExecutionConfig.GlobalJobParameters gloJobParams = exeCfg.getGlobalJobParameters();
        Configuration gloCfg = (Configuration) gloJobParams;
        limit = gloCfg.getLong("limit", 0);
        System.out.println("limit=" + limit);
    }

    @Override
    public boolean filter(Long value) throws Exception {
        return value > limit;
    }

}
