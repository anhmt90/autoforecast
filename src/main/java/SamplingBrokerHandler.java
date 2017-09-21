import io.moquette.interception.AbstractInterceptHandler;
import io.moquette.interception.messages.InterceptPublishMessage;
import io.moquette.interception.messages.InterceptSubscribeMessage;
import io.moquette.interception.messages.InterceptUnsubscribeMessage;
import io.moquette.proto.messages.AbstractMessage;
import io.moquette.proto.messages.PublishMessage;
import io.moquette.server.Server;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.toubassi.femtozip.ArrayDocumentList;
import org.toubassi.femtozip.models.FemtoZipCompressionModel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Dictionary;
import java.util.Hashtable;

/**
 * Created by chris on 19.02.16.
 */
public class SamplingBrokerHandler extends AbstractInterceptHandler {

    private final Server mqttBroker;
    private final CircularFifoQueue<byte[]> cfb;
    private final Dictionary<Byte, FemtoZipCompressionModel> dictionaries;
    private long cnt;
    private byte dictionaryId;
    private boolean createDictionary;

    public SamplingBrokerHandler(Server mqttBroker) {
        this.mqttBroker = mqttBroker;
        this.cfb = new CircularFifoQueue<>(500);
        dictionaries = new Hashtable<>();
        cnt = 0;
        dictionaryId = 1;
        createDictionary = false;
    }

    private int calcDictionarySize() {
        return 190; //todo make better
    }

    @Override
    public void onPublish(InterceptPublishMessage msg) {
        System.out.println("#onpublish:" + msg.getTopicName() + " #payload:" + msg.getPayload().array().length + "#cnt:" + cnt);

        if(msg.getTopicName().equalsIgnoreCase(Const.TOPIC_NAME)) {

            byte[] notification = msg.getPayload().array();
            byte header = notification[0];
            byte[] payload = null;
            cnt++;

            if (header > -2) {
                if (cnt % 20 == 0) {
                    try {
                        byte[] msgNo = new byte[5];
                        msgNo[0] = -2;
                        System.arraycopy(notification, 1, msgNo, 1, 4);

                        PublishMessage pm = new PublishMessage();
                        pm.setTopicName(Const.REPLY_TOPIC_NAME);
                        pm.setPayload(ByteBuffer.wrap(msgNo));
                        pm.setQos(AbstractMessage.QOSType.LEAST_ONE);
                        mqttBroker.internalPublish(pm);

                        System.out.println("reply msg No : " + ByteBuffer.wrap(Arrays.copyOfRange(msgNo, 1, 5)).getInt());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                payload = Arrays.copyOfRange(notification, 5, notification.length);
            }

            if(header == -100) {
                createDictionary = true;
                System.out.println("#control-msg:" + header);
            }
            else if (header < -98 ) {
                System.out.println("#control-msg:" + header);
                return;
            }

            if(header == -1) { //if -1 it's uncompressed
//                byte[] msgNo = Arrays.copyOfRange(payload, 4, payload.length);
                cfb.add(payload);
//                cfb.add(Arrays.copyOfRange(payload, 5, payload.length));
            }
            else if(header > 0) { //if positive, it represents a new dictionary ID
                FemtoZipCompressionModel femtoZipCompressionModel = this.dictionaries.get(header);
                byte[] decompressed = femtoZipCompressionModel.decompress(payload);
//                byte[] decompressed = femtoZipCompressionModel.decompress(payload);
                cfb.add(decompressed);
            }

            if(createDictionary) { //every 100 messages we resample
                try {
                    FemtoZipCompressionModel femtoZipCompressionModel = new FemtoZipCompressionModel();
//                    if (!FemtoFactory.isCachedDictionaryAvailable()) {
//                        System.out.println("#sampling-a-dictionary");
//
                        ArrayList<byte[]> temp = new ArrayList<>(cfb.size());

                        for(int i = 0; i < cfb.size(); i++) {
                            temp.add(cfb.get(i));
                        }

                        //Build the dictionary
                        femtoZipCompressionModel.setMaxDictionaryLength(calcDictionarySize());
                        femtoZipCompressionModel.build(new ArrayDocumentList(temp));

                        System.out.println("#sampling-complete");

//                        Caching the sampled dictionary
//                        FemtoFactory.toCache(femtoZipCompressionModel);
//                        System.out.println("Caching dictionary");
//                    } else {
//                        System.out.println("Loading dictionary from cache");
//                        femtoZipCompressionModel = FemtoFactory.fromCache();
//                    }

                    this.dictionaries.put(dictionaryId, femtoZipCompressionModel);
                    byte[] dictionary = FemtoFactory.getDictionary(femtoZipCompressionModel);;

                    //publish the dictionary
                    byte[] dictMesage = new byte[dictionary.length +1];
                    dictMesage[0] = dictionaryId;
                    dictionaryId = (byte)((dictionaryId+1) % 127);
                    for(int i = 0; i < dictionary.length; i++){
                        dictMesage[i+1] = dictionary[i];
                    }

                    byte[] cmdb = new byte[1];
                    cmdb[0] = -103;
                    PublishMessage cmd = new PublishMessage();
                    cmd.setTopicName(Const.DICT_TOPIC_NAME);
                    cmd.setPayload(ByteBuffer.wrap(cmdb));
                    cmd.setQos(AbstractMessage.QOSType.LEAST_ONE);
                    this.mqttBroker.internalPublish(cmd);


                    PublishMessage pm = new PublishMessage();
                    pm.setTopicName(Const.DICT_TOPIC_NAME);
                    pm.setPayload(ByteBuffer.wrap(dictMesage));
                    pm.setQos(AbstractMessage.QOSType.LEAST_ONE);
                    this.mqttBroker.internalPublish(pm);
                    System.out.println("finished sampling dictionary");
                } catch (IOException e) {
                    e.printStackTrace();
                }
                createDictionary = false;
            }
        }
    }

    @Override
    public void onSubscribe(InterceptSubscribeMessage msg) {
        System.out.println("#onsubscribe: " + msg.getTopicFilter() + " #clientID:" + new String(msg.getClientID()));
    }

    public void onUnsubscribe(InterceptUnsubscribeMessage msg) {
        System.out.println("TODO");
    }
}
