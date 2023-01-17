import org.apache.kafka.clients.producer.KafkaProducer;

public class Test {
    public static void main(String[] args) {
        String name = KafkaProducer.class.getCanonicalName();
        System.out.println("name: "+name);

        String username="zhangsan";
        int age=20;
        //System.out.printf("姓名:%s,年龄:%d",username,age);

        String str="姓名:%s,年龄:%d";
        System.out.println(String.format(str,username,age));

        int maxNumber = 2147483647;
        System.out.println(maxNumber);
    }
}
