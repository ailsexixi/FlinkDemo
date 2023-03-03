package utils;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author 1111358@cecdat.com
 * @version 1.0.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class HotItem {
    private Long itemId;
    private Long count;
    private Long windowEndTime;
}
