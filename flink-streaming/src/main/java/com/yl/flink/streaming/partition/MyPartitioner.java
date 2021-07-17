package com.yl.flink.streaming.partition;

import org.apache.flink.api.common.functions.Partitioner;

public class MyPartitioner implements Partitioner<String> {

    @Override
    public int partition(String key, int numPartitions) {
        System.out.println("key=" + key);
        if (key.contains("185")) {
            return 0;
        } else if (key.contains("155")) {
            return 1;
        } else {
            return 2;
        }
    }

}
