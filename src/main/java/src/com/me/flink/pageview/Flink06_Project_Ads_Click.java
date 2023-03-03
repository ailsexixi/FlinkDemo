package src.com.me.flink.pageview;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import utils.AdsClickLog;

/**
 * @author 1111358@cecdat.com
 * @version 1.0.0
 */

//电商网站的市场营销商业指标中，除了自身的APP推广，还会考虑到页面上的广告投放（包括自己经营的产品和其它网站的广告）。所以广告相关的统计分析，也是市场营销的重要指标。
//	对于广告的统计，最简单也最重要的就是页面广告的点击量，网站往往需要根据广告点击量来制定定价策略和调整推广方式，而且也可以借此收集用户的偏好信息。
// 更加具体的应用是，我们可以根据用户的地理位置进行划分，从而总结出不同省份用户对不同广告的偏好，这样更有助于广告的精准投放
//某电商网站的广告点击日志数据AdClickLog.csv,	本日志数据文件中包含了某电商网站一天用户点击广告行为的事件流，数据集的每一行表示一条用户广告点击行为，由用户ID、广告ID、省份、城市和时间戳组成并以逗号分隔。
//统计各省份广告点击量
public class Flink06_Project_Ads_Click {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .readTextFile("D:\\flink\\FlinkDemo\\src\\main\\resources\\AdClickLog.csv")
                .map(line -> {
                    String[] datas = line.split(",");
                    //返回的是类，要写return
                    return new AdsClickLog(
                            Long.valueOf(datas[0]),
                            Long.valueOf(datas[1]),
                            datas[2],
                            datas[3],
                            Long.valueOf(datas[4]));
                })
                .map(log -> Tuple2.of(Tuple2.of(log.getProvince(),log.getAdId()),1L))
                .returns(Types.TUPLE(Types.TUPLE(Types.STRING,Types.LONG),Types.LONG))
//                .keyBy(t -> t.f0) //lambda 表达式不支持
                .keyBy(new KeySelector<Tuple2<Tuple2<String,Long>,Long>, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> getKey(Tuple2<Tuple2<String, Long>, Long> t) throws Exception {
                        return  t.f0;
                    }
                })
                .sum(1)
                .print();
        env.execute();

    }
}
