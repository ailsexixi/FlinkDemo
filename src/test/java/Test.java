import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Random;

/**
 * @author 1111358@cecdat.com
 * @version 1.0.0
 */
public class Test {

    public static void main(String[] args) throws JsonProcessingException {
//        for (int i = 0; i < 10; i++) {
//            System.out.println(("uv_" + new Random().nextInt(8)).substring(0,2));
//        }

        String jsonStr = "{\"txId\":\"dadan\",\"payChannel\":\"asdavava\",\"eventTime\":1234,\"test\":\"1\",\"pay\":\"dafsdf\"}";
//        String jsonStr = "{\"txId\":\"dadan\",\"payChannel\":\"asdavava\"}";
        System.out.println(jsonStr);

        ObjectMapper mapper = new ObjectMapper();
//        mapper.disable(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES);
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        TxEvent txEvent = mapper.readValue(jsonStr, TxEvent.class);
        System.out.println(txEvent);
    }
}
