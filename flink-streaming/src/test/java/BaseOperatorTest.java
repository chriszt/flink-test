import com.yl.flink.streaming.operator.BaseOperator;
import org.junit.Test;

public class BaseOperatorTest {

    public final static String FILEPATH = "/home/yl/proj/flink-test/flink-streaming/src/main/resources/BaseTextInput.txt";

    @Test
    public void testReadTextFile() throws Exception {
        new BaseOperator().readTextFile(FILEPATH);
    }

    @Test
    public void testReadFile() throws Exception {
        new BaseOperator().readFile(FILEPATH);
    }

    @Test
    public void testWriteToScreen() throws Exception {
        new BaseOperator().writeToScreen();
    }

    @Test
    public void testMapTemplate() throws Exception {
        new BaseOperator().mapTemplate();
    }

    @Test
    public void testFilterTemplate() throws Exception {
        new BaseOperator().filterTemplate();
    }

    @Test
    public void testKeyByTemplate() throws Exception {
        new BaseOperator().keyByTemplate();
    }

    @Test
    public void testReduceTemplate() throws Exception {
        new BaseOperator().reduceTemplate();
    }

    @Test
    public void testAggreTemplate() throws Exception {
        new BaseOperator().aggreTemplate();
    }

    @Test
    public void testSideOutputTemplate() throws Exception {
        new BaseOperator().sideOutputTemplate();
    }

    @Test
    public void testProjectTemplate() throws Exception {
        new BaseOperator().projectTemplate();
    }

    @Test
    public void testUnionTemplate() throws Exception {
        new BaseOperator().unionTemplate();
    }

    @Test
    public void testCoMapTemplate() throws Exception {
        new BaseOperator().coMapTemplate();
    }

    @Test
    public void testCoFlatMapTemplate() throws Exception {
        new BaseOperator().coFlatMapTemplate();
    }

    @Test
    public void testIterateTemplate() throws Exception {
        new BaseOperator().iterateTemplate();
    }

}
