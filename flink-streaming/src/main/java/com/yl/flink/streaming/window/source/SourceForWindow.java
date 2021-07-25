package com.yl.flink.streaming.window.source;

import com.yl.flink.streaming.window.util.TimeUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class SourceForWindow implements SourceFunction<Tuple3<String, Integer, String>> {

    private volatile boolean isRunning = true;

    private long sleepTime;

    private boolean stopSession = false;

    public static final String[] WORDS = new String[]{
            "chriszt",
            "chriszt",
            "chriszt",
            "chriszt",
            "chriszt",
            "chriszt",
            "chriszt",
            "chriszt",
            "chriszt",
            "chriszt",
//            "java",
//            "flink",
//            "flink",
//            "flink",
//            "chriszt",
//            "chriszt",
//            "hadoop",
//            "hadoop",
//            "spark"
    };

    public SourceForWindow() {

    }

    public SourceForWindow(long sleepTime) {
        this.sleepTime = sleepTime;
    }

    public SourceForWindow(long sleepTime, boolean stopSession) {
        this.sleepTime = sleepTime;
        this.stopSession = stopSession;
    }

    @Override
    public void run(SourceContext<Tuple3<String, Integer, String>> ctx) throws Exception {
        int count = 0;
        while (isRunning) {
            String word = WORDS[count % WORDS.length];
            String time = TimeUtils.getHHmmss(System.currentTimeMillis());
            Tuple3<String, Integer, String> item = new Tuple3<>(word, count, time);
            ctx.collect(item);
            System.out.println("Send item: " + item);
            if(stopSession && count == WORDS.length) {
                Thread.sleep(10000);
            } else {
                Thread.sleep(sleepTime);
            }
            count++;
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    public static void main(String[] args) {
//        Tuple3<String, Integer, String> t = new Tuple3<>("aaa", 10, "111");
        Tuple3<String, Integer, String> t = Tuple3.of("aaa", 10, "111");
        System.out.println(t);

    }
}
