//package publisher;

import org.apache.commons.io.LineIterator;
import org.eclipse.paho.client.mqttv3.*;
import org.toubassi.femtozip.models.FemtoZipCompressionModel;
import samplingBroker.Const;
import samplingBroker.FemtoFactory;

import java.util.Arrays;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.Hashtable;

import com.satori.rtm.*;
import com.satori.rtm.model.*;

import static org.apache.commons.lang3.math.NumberUtils.max;

/**
 * Created by chris on 19.02.16.
 */
public class PublisherHandler implements MqttCallback, Runnable {

    private final MqttClient cl;
    private final LineIterator it;
    private final Dictionary<Byte, FemtoZipCompressionModel> dictionaries; //TODO why storing many dictionaries?
    private int compressedCnt;
    private int uncompressedCnt;

    static final String endpoint = "wss://open-data.api.satori.com";
    static final String appkey = "6e3edcFe4dEE7d232F2fc6ae0cE58CEb";
    static final String channel = "Meetup-RSVP";


    public PublisherHandler(MqttClient cl, LineIterator it)  {
        this.cl = cl;
        this.it = it;
        dictionaries = new Hashtable<>();
//        compressedCnt = 1;
//        uncompressedCnt = 1;
    }

    //subscribe to DICT_TOPIC_NAME to receive dicts from the SB
    public void subscribeDict() throws MqttException {
        this.cl.subscribe(Const.DICT_TOPIC_NAME);
        this.cl.subscribe(this.cl.getClientId());
    }
    /*##################################################################*/
    public void publishMessages() throws Exception {

        /*
        * Receiving messages from satori.com portal and re-publish them
        * */
        final RtmClient client = new RtmClientBuilder(endpoint, appkey)
                .setListener(new RtmClientAdapter() {
                    @Override
                    public void onEnterConnected(RtmClient client) {
                        System.out.println("Connected to Satori RTM!");
                    }
                }).build();

        SubscriptionAdapter listener = new SubscriptionAdapter() {
            @Override
            public void onSubscriptionData(SubscriptionData data) {
                for (AnyJson json : data.getMessages()) {
//                    System.out.println("Got message: " + json.toString());
                    try{
                        byte[] payload = json.toString().getBytes();
                        boolean empty = false;
                        synchronized (dictionaries) {
                            empty = dictionaries.isEmpty();
                        }
                        /*<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<  Non-Compression >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>*/
                        if(empty) { //there is no dict available
                            byte[] publishMessage = new byte[payload.length + 1];
                            publishMessage[0] = -1;                //-1: code to indicate that message payload is uncompressed
                            System.arraycopy(payload, 0, publishMessage, 1, payload.length);
                            cl.publish(Const.TOPIC_NAME, new MqttMessage(publishMessage));
                            uncompressedCnt++;
                            System.out.println("#"+uncompressedCnt+" UNCOMP " + publishMessage.length + " bytes");

                        }
                        /*<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<  Compression >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>*/
                        else if(!empty) {
                            byte latestDictId = 0;
                            FemtoZipCompressionModel femtoZipCompressionModel;
                            synchronized (dictionaries) {
                                Enumeration<Byte> keys = dictionaries.keys();
                                while (keys.hasMoreElements()) {
                                    latestDictId = max(keys.nextElement(), latestDictId);
                                }
                                femtoZipCompressionModel = dictionaries.get(latestDictId);
                            }

                            byte[] compressedPayload = femtoZipCompressionModel.compress(payload);
                            byte[] publishMessage = new byte[compressedPayload.length + 1];
                            publishMessage[0] = latestDictId;   //header is the Id of the currently used dictionary
                            System.arraycopy(compressedPayload, 0, publishMessage, 1, compressedPayload.length);

                            cl.publish(Const.TOPIC_NAME, new MqttMessage(publishMessage));
                            System.out.println("#"+compressedCnt+" COMP("+publishMessage[0]+"): " + publishMessage.length + " bytes");
                            compressedCnt++;
                        }
                        Thread.sleep(50);
                    } catch (Exception e){
                        e.printStackTrace();
                    }

                }
            }
        };

        client.createSubscription(channel, SubscriptionMode.SIMPLE, listener);

        client.start();
    }


    @Override
    public void connectionLost(Throwable throwable) {
        System.out.println("Publisherhandler: connectionLost");
        throwable.printStackTrace();
    }

    @Override
    public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
        byte id = mqttMessage.getPayload()[0];
//        if(id < -90) {
//            return;         //the message arrived is not a dictionary -> do nothing here
//        }
        byte[] payload = Arrays.copyOfRange(mqttMessage.getPayload(), 1, mqttMessage.getPayload().length);

        //receive and store the new dict into 'dictionaries'
        if(topic.equalsIgnoreCase(Const.DICT_TOPIC_NAME)) {
            System.out.println("#got-new-dictionary: " + payload.length + " bytes");
            FemtoZipCompressionModel femtoZipCompressionModel1 = FemtoFactory.fromDictionary(payload);
            dictionaries.put(id, femtoZipCompressionModel1);
            System.out.println("Pub: new dictionary received");
            Thread.sleep(1200);
        }
        else if(topic.equalsIgnoreCase(this.cl.getClientId())) {
            System.out.println("#got-cached-dictionary: " + payload.length + " bytes");
            FemtoZipCompressionModel femtoZipCompressionModel1 = FemtoFactory.fromDictionary(payload);
            dictionaries.put(id, femtoZipCompressionModel1);
            System.out.println("Sub: cached dictionary received");
//            this.cl.unsubscribe(this.cl.getClientId());
        }
        else if(topic.equalsIgnoreCase(Const.TOPIC_NAME)) {
            if(id == -1) {
                System.out.println("#UNCOMP:" + payload.length);
            }
            else if (id >= 0) {
                FemtoZipCompressionModel femtoZipCompressionModel = dictionaries.get(id);
                byte[] decompressedMessage = femtoZipCompressionModel.decompress(payload);
                System.out.println("#COMP: "+payload.length+" - #DECOMP: " + decompressedMessage.length);
            }
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

    }
    @Override
    public void run() {
        try {
            publishMessages();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
