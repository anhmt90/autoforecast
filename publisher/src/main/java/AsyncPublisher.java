//import org.apache.commons.io.FileUtils;
//import org.apache.commons.io.LineIterator;
//import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
//import org.eclipse.paho.client.mqttv3.MqttException;
//import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
//
//import java.io.File;
//
///**
// * Created by Tanuj on 3/3/2016.
// */
//public class AsyncPublisher {
//    public static void main(String[] args) throws Exception {
//
//        LineIterator it = FileUtils.lineIterator(new File("D:\\Projects\\datasets\\debs2015\\debs2015.xml"), "UTF-8");
//
//        //MqttClientPersistence s_dataStore;
//        MqttAsyncClient cl = new MqttAsyncClient("tcp://131.159.52.29:1883", "Publisher", new MemoryPersistence());
//
//        AsyncPublisherHandler publisherHandler = new AsyncPublisherHandler(cl, it);
//        cl.setCallback(publisherHandler);
//        cl.connect();
//
//        publisherHandler.subscribe();
//
//        Thread th = new Thread(publisherHandler);
//        th.run();
//
//
//        Runtime.getRuntime().addShutdownHook(new Thread() {
//            @Override
//            public void run() {
//                System.out.println("Stopping broker");
//                try {
//                    cl.disconnect();
//                } catch (MqttException e) {
//                    e.printStackTrace();
//                }
//                System.out.println("SamplingBroker stopped");
//            }
//        });
//
//        Thread.sleep(Long.MAX_VALUE);
//
//        System.out.println("Quit");
//    }
//}
