package src.com.me.flink.pageview;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http2.Http2Exception;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import utils.MarketingUserBehavior;
import utils.UserBehavior;


import java.util.*;

/**
 * @author 1111358@cecdat.com
 * @version 1.0.0
 */
//随着智能手机的普及，在如今的电商网站中已经有越来越多的用户来自移动端，相比起传统浏览器的登录方式，手机APP成为了更多用户访问电商网站的首选。对于电商企业来说，
// 一般会通过各种不同的渠道对自己的APP进行市场推广，而这些渠道的统计数据（比如，不同网站上广告链接的点击量、APP下载量）就成了市场营销的重要商业指标。
//APP市场推广统计 - 分渠道，统计不通渠道上有关于app的用户下载、卸载等行为
//自定义Source
//java 创建对象时，不能用var点出来
public class Flink04_Project_AppAnalysis_By_Chanel {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .addSource(new AppMarketingDataSource())
                .map(behavior -> Tuple2.of(behavior.getChannel() + "_" + behavior.getBehavior(),1L))
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                .keyBy(t -> t.f0)
                .sum(1)
                .print();
        env.execute();
    }

    public static class AppMarketingDataSource implements SourceFunction<MarketingUserBehavior> {

        public Boolean isRunning = true;
        Random random = new Random();
        List<String> channels = Arrays.asList("huawwei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");
        List<String> behavior = Arrays.asList("download", "install", "update", "uninstall");

        @Override
        public void run(SourceContext<MarketingUserBehavior> scx) throws Exception {
            while (isRunning){
            MarketingUserBehavior mu = new MarketingUserBehavior(
                    Long.valueOf(random.nextInt(100000)),
                    behavior.get(random.nextInt(behavior.size())),
                    channels.get(random.nextInt(channels.size())),
                    System.currentTimeMillis()
            );
            scx.collect(mu);
            Thread.sleep(500);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
