package com.yl.flink.streaming.connector.source;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

public class TextFileSource {

    public void testFileSrc(String filePath) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        String filePath = "/home/yl/proj/flink-test/TextFileSource.txt";
        TextInputFormat fmt = new TextInputFormat(new Path(filePath));
        TypeInformation<String> typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
        DataStreamSource<String> dataStream = env.readFile(fmt, filePath,
                FileProcessingMode.PROCESS_CONTINUOUSLY, 1000, typeInfo);

        dataStream.print();
        env.execute("TextFileSource");
    }

}
