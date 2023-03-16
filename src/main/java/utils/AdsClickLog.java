package utils;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author 1111358@cecdat.com
 * @version 1.0.0
 */
@Data   // 注在类上，提供类的get、set、equals、hashCode、toString等方法
@AllArgsConstructor  //注在类上，提供类的全参构造
@NoArgsConstructor  //注在类上，提供类的无参构造
public class AdsClickLog {
    private Long userId;
    private Long adId;
    private String province;
    private String city;
    private Long timestamp;

}
