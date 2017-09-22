import publisher.Publisher;
import samplingBroker.SamplingBroker;
import subscriber.Subscriber;

public class Driver {
    public static void main(String[] args) throws Exception {
        System.out.println("<<<<<<<<<<<<<<<<<<<< Starting Driver >>>>>>>>>>>>>>>>>>>>");
        Thread th = new Thread();
        th.start();
        SamplingBroker.main(new String[] {});
        Subscriber.main(new String[] {});
        Publisher.main(new String[] {});
    }
}
