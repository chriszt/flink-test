package com.yl.flink.streaming.partition;

import com.yl.flink.streaming.beans.Trade;
import org.apache.flink.api.common.functions.Partitioner;

public class MyTradePartitioner implements Partitioner<Trade> {

    @Override
    public int partition(Trade key, int numPartitions) {
        if (key.getCardNum().contains("185") && key.getTrade() > 1000) {
            return 0;
        } else if (key.getCardNum().contains("155") && key.getTrade() > 1150) {
            return 1;
        } else {
            return 2;
        }
    }

}
