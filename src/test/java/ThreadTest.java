import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadTest {
    //static CountDownLatch cdl = new CountDownLatch(2);
    static AtomicInteger ai = new AtomicInteger(10);

    public static void main(String[] args) throws InterruptedException {
        /*ExecutorService exe = Executors.newFixedThreadPool(9);
        for(int i=0;i<100;i++){
            exe.execute(new Runnable() {
                @Override
                public void run() {
                    System.out.println(Thread.currentThread().getName()+":"+ai.getAndIncrement());
                    //cdl.countDown();
                }
            });
        }*/
        //cdl.await();
        System.out.println(ai.get());
        //exe.shutdown();

        result();
    }

    public static void result() {
        AtomicInteger tai = new AtomicInteger(10);
        for (int idx = 0; idx < 100; idx++) {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    System.out.println(Thread.currentThread().getName() + ":" + tai.getAndIncrement());
                }
            });
            thread.start();
        }
    }
}
