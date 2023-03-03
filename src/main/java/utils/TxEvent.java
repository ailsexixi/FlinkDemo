package utils;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author 1111358@cecdat.com
 * @version 1.0.0
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TxEvent {
    @JsonProperty("txId")
    private String txId;

    @JsonProperty("payChannel")
    private String payChannel;

    @JsonProperty("eventTime")
    private Long eventTime;
}
