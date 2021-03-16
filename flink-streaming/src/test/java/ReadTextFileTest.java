import com.yl.flink.streaming.connector.source.TextFileSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;


public class ReadTextFileTest {

//    private Logger log = LoggerFactory.getLogger(ReadTextFileTest.class);

    public final static String FILEPATH = "/home/yl/proj/flink-test/flink-streaming/TextFileSource.txt";

    @Test
    public void readText() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = env.readTextFile(FILEPATH);
        dataStream.print();
        env.execute();
    }

    @Test
    public void testFileSrcTest() throws Exception {
        new TextFileSource().testFileSrc(FILEPATH);
    }

}
