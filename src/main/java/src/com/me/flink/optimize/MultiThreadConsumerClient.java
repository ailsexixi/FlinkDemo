package src.com.me.flink.optimize;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author 1111358@cecdat.com
 * @version 1.0.0
 */
public class MultiThreadConsumerClient implements Runnable {

    private LinkedBlockingQueue<String> bufferQueue;
    private CyclicBarrier cyclicBarrier;

    public MultiThreadConsumerClient(LinkedBlockingQueue<String> bufferQueue, CyclicBarrier cyclicBarrier) {
        this.bufferQueue = bufferQueue;
        this.cyclicBarrier = cyclicBarrier;
    }

    @Override
    public void run() {
        String entity;
        while (true){
            // 从 bufferQueue 的队首消费数据
            if(bufferQueue.isEmpty() && cyclicBarrier.getNumberWaiting() > 0){
                //执行当前线程的flush操作
                try {
                    cyclicBarrier.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (BrokenBarrierException e) {
                    e.printStackTrace();
                }
            }
            entity = bufferQueue.poll();
            // 执行 client 消费数据的逻辑
            doSomething(entity);
        }
    }

    // client 消费数据的逻辑
    private void doSomething(String entity) {
        // client 积攒批次并调用第三方 api
    }
}