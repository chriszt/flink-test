package com.yl.flink.streaming.param;

import org.apache.flink.api.common.functions.FilterFunction;

public class FilterConstructed implements FilterFunction<Long> {

    private ParamBean pb = null;

    public FilterConstructed() {
    }

    public FilterConstructed(ParamBean pb) {
        this.pb = pb;
    }

    @Override
    public boolean filter(Long value) throws Exception {
        return value > pb.getFlag();
    }

}
