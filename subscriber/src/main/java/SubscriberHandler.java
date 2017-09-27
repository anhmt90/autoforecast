import org.eclipse.paho.client.mqttv3.*;
import org.toubassi.femtozip.models.FemtoZipCompressionModel;
import samplingBroker.Const;
import samplingBroker.FemtoFactory;

import java.util.Arrays;
import java.util.Dictionary;
import java.util.Hashtable;

/**
 * Created by chris on 19.02.16.
 */
public class SubscriberHandler implements MqttCallback, Runnable{
    private final MqttClient cl;
    private final Dictionary<Byte, FemtoZipCompressionModel> dictionaries;
    private long uncompressedCnt;
    private long compressedCnt;

    public SubscriberHandler(MqttClient cl) {
        this.cl = cl;
        dictionaries = new Hashtable<>();
        uncompressedCnt = 1;
        compressedCnt = 1;
    }

    public void startSubscribing() throws MqttException {
        this.cl.subscribe(Const.DICT_TOPIC_NAME);
        this.cl.subscribe(Const.TOPIC_NAME);
        this.cl.subscribe(this.cl.getClientId());
        System.out.println("subscribed to " + Const.TOPIC_NAME);

    }

    @Override
    public void connectionLost(Throwable throwable) {
        System.out.println("Connection to Broker lost!");
//        try {
//            this.cl.connect();
//        } catch (MqttException e) {
//            e.printStackTrace();
//        }

    }

    @Override
    public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
        byte[] msgPayload = mqttMessage.getPayload();
        byte header = msgPayload[0];
        byte[] payload = Arrays.copyOfRange(msgPayload, 1, msgPayload.length);
//        if(header == -103){
//            System.out.println("Sub: cmdb received");
//        }
        if(topic.equalsIgnoreCase(Const.DICT_TOPIC_NAME)) {
            System.out.println("#got-new-dictionary: " + payload.length + " bytes");
            FemtoZipCompressionModel femtoZipCompressionModel1 = FemtoFactory.fromDictionary(payload);
            dictionaries.put(header, femtoZipCompressionModel1);
            System.out.println("Sub: new dictionary received");
        }
        else if(topic.equalsIgnoreCase(this.cl.getClientId())) {
            System.out.println("#got-cached-dictionary: " + payload.length + " bytes");
            FemtoZipCompressionModel femtoZipCompressionModel1 = FemtoFactory.fromDictionary(payload);
            dictionaries.put(header, femtoZipCompressionModel1);
            System.out.println("Sub: cached dictionary received");
//            this.cl.unsubscribe(this.cl.getClientId());
        }
        else if(topic.equalsIgnoreCase(Const.TOPIC_NAME)) {
            if(header == -1) {
                System.out.println("Sub: #UNCOMP" + uncompressedCnt + ": " + (payload.length+1) + " bytes");
                uncompressedCnt++;
            }
            else if (header >= 0) {
                FemtoZipCompressionModel femtoZipCompressionModel = dictionaries.get(header);
                byte[] decompressedMessage = femtoZipCompressionModel.decompress(payload);
                System.out.println("Sub: #" + compressedCnt+ " COMP: " + (payload.length+1) + " bytes - DECOMP: " + decompressedMessage.length + "bytes");
                compressedCnt++;
            }
        }

    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

    }

    @Override
    public void run() {
        try {
            startSubscribing();
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }
}
