//package publisher;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.File;

/**
 * Created by chris on 19.02.16.
 */
public class Publisher {

    public static void main(String[] args) throws Exception {
        System.out.println("<<<<<<<<<<<<<<<<<<<< Starting Publisher >>>>>>>>>>>>>>>>>>>>");
        LineIterator it = FileUtils.lineIterator(new File("/mnt/A43003F9E520D223/Workplace/_data/meetup.json"), "UTF-8");


        //MqttClientPersistence s_dataStore;
//        MqttClient cl = new MqttClient("tcp://131.159.52.29:1883", "Publisher", new MemoryPersistence());
        MqttClient cl = new MqttClient("tcp://127.0.0.1:1883", "publisher1", new MemoryPersistence());

        PublisherHandler publisherHandler = new PublisherHandler(cl);
        cl.setCallback(publisherHandler);
        cl.connect();
        publisherHandler.subscribeDict();

        Thread th = new Thread(publisherHandler);
        th.start();

        System.out.println("Publisher started press [CTRL+C] to stop");
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("Stopping Publisher");
                try {
                    cl.disconnect();
                } catch (MqttException e) {
                    e.printStackTrace();
                }
                System.out.println("Publisher stopped");
            }
        });

        Thread.sleep(Long.MAX_VALUE);

        System.out.println("Quit");
    }
}
