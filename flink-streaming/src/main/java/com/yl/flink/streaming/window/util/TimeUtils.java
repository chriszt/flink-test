package com.yl.flink.streaming.window.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeUtils {

    public static String getHHmmss(Long time) {
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        String str = sdf.format(new Date(time));
        return "时间：" + str;
    }

    public static void main(String[] args) {
        System.out.println(TimeUtils.getHHmmss(System.currentTimeMillis()));
    }

}
