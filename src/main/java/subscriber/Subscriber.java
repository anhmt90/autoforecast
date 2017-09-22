package subscriber;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/**
 * Created by chris on 19.02.16.
 */
public class Subscriber {
    public static void main(String[] args) throws MqttException, InterruptedException {

        MqttClient cl = new MqttClient("tcp://localhost:1883", "Subscriber", new MemoryPersistence());

        SubscriberHandler susbscriberHandler = new SubscriberHandler(cl);
        cl.setCallback(susbscriberHandler);
        cl.connect();
        susbscriberHandler.startSubscribing();

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
}