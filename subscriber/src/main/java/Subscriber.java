import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/**
 * Created by chris on 19.02.16.
 */
public class Subscriber {
    public static void main(String[] args) throws MqttException, InterruptedException {
        System.out.println("<<<<<<<<<<<<<<<<<<<< Starting Subscriber >>>>>>>>>>>>>>>>>>>>");

//        MqttClient cl = new MqttClient("tcp://localhost:1883", "Subscriber", new MemoryPersistence());
        MqttClient cl = new MqttClient("tcp://127.0.0.1:1883", "subscriber#1", new MemoryPersistence());

        SubscriberHandler susbscriberHandler = new SubscriberHandler(cl);
        cl.setCallback(susbscriberHandler);
        cl.connect();
//        susbscriberHandler.startSubscribing();

        Thread th = new Thread(susbscriberHandler);
        th.start();

        //Bind  a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("Stopping Subscriber");
                try {
                    cl.disconnect();
                } catch (MqttException e) {
                    e.printStackTrace();
                }
                System.out.println("Subscriber stopped");
            }
        });

        Thread.sleep(Long.MAX_VALUE);

        System.out.println("Quit");
    }

//    @Override
//    public void run() {
//        try {
//            this.main(new String[] {});
//        } catch (MqttException e) {
//            e.printStackTrace();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
}