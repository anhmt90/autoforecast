package samplingBroker;

import io.moquette.interception.AbstractInterceptHandler;
import io.moquette.interception.messages.InterceptPublishMessage;
import io.moquette.interception.messages.InterceptSubscribeMessage;
import io.moquette.interception.messages.InterceptUnsubscribeMessage;
//import io.moquette.proto.messages.AbstractMessage;
//import io.moquette.proto.messages.PublishMessage;
import io.moquette.parser.proto.messages.PublishMessage;
import io.moquette.parser.proto.messages.AbstractMessage;
import io.moquette.server.Server;
//import io.netty.handler.codec.mqtt.*;
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
    private long uncompressedCnt;
    private long compressedCnt;
    private byte dictionaryId;
    private boolean createDictionary;

    public SamplingBrokerHandler(Server mqttBroker) {

        this.mqttBroker = mqttBroker;
        this.cfb = new CircularFifoQueue<>(500);
        dictionaries = new Hashtable<>();
        uncompressedCnt = 1;
        compressedCnt = 1;
        dictionaryId = 1;
        createDictionary = false;
    }

    private int calcDictionarySize() {
        return 190; //todo make better
    }

    @Override
    public void onPublish(InterceptPublishMessage msg) {
        if(msg.getTopicName().equalsIgnoreCase(Const.TOPIC_NAME)) {
            byte[] notification = msg.getPayload().array();
            byte header = notification[0];

            byte[] payload = Arrays.copyOfRange(notification, 1, notification.length);

            //-100: code to indicate ending of UNCOMPRESSED stream
            if(header == -100) {
                createDictionary = true;
                System.out.println("#control-msg:" + header + " >>>>>>>>>>>>");
            }
            else if (header < -98 ) {
                System.out.println("#control-msg:" + header + " >>>>>>>>>>>>");
                return;
            }

            if(header == -1) { //if -1 it's uncompressed
                cfb.add(payload);
                System.out.println("#UNCOMP" +  uncompressedCnt + " onPublish:" + msg.getPayload().array().length + "bytes onTopic:" + msg.getTopicName());
                ++uncompressedCnt;
            }
            else if(header > 0) { //if positive, it represents a new dictionary ID dictionaryId
//                FemtoZipCompressionModel femtoZipCompressionModel = this.dictionaries.get(header);
//                byte[] decompressed = femtoZipCompressionModel.decompress(payload);
//                cfb.add(decompressed);
                System.out.println("#COMP" + compressedCnt + " onPublish:" + msg.getPayload().array().length + "bytes onTopic:" + msg.getTopicName());
                ++compressedCnt;
            }

            if(createDictionary) { //every 100 messages we resample
                System.out.println("Creating dictionary .................");
                byte[] dictionary = new byte[0];
                try {
                    FemtoZipCompressionModel femtoZipCompressionModel = new FemtoZipCompressionModel();

                    if (!FemtoFactory.isCachedDictionaryAvailable()) {
                        System.out.println("#sampling-a-dictionary");

                        ArrayList<byte[]> temp = new ArrayList<>(cfb.size());

                        for(int i = 0; i < cfb.size(); i++) {
                            temp.add(cfb.get(i));
                        }

                        //Build the dictionary
                        femtoZipCompressionModel.setMaxDictionaryLength(calcDictionarySize()); //Todo make it better
                        femtoZipCompressionModel.build(new ArrayDocumentList(temp));

                        System.out.println("#sampling-complete");

                        //Caching the sampled dictionary
                        System.out.println("Caching dictionary");
                        FemtoFactory.toCache(femtoZipCompressionModel);

                        //get the sampled dict
                        dictionary = FemtoFactory.getDictionary(femtoZipCompressionModel);

                    } else {
                        System.out.println("Loading dictionary from cache");
//                        femtoZipCompressionModel = FemtoFactory.fromCache();
                        dictionary = FemtoFactory.fromCache();
                    }



//                    old one
//                    System.out.println("#sampling-a-dictionary");
//                    ArrayList<byte[]> temp = new ArrayList<>(cfb.size());
//                    for(int i = 0; i < cfb.size(); i++) {
//                        temp.add(cfb.get(i));
//                    }
//
//                    //Build the dictionary
//                    FemtoZipCompressionModel femtoZipCompressionModel = new FemtoZipCompressionModel();
//                    femtoZipCompressionModel.setMaxDictionaryLength(calcDictionarySize());
//                    femtoZipCompressionModel.build(new ArrayDocumentList(temp));

//                    this.dictionaries.put(dictionaryId, femtoZipCompressionModel);
//                    byte[] dictionary = FemtoFactory.getDictionary(femtoZipCompressionModel);

                    //publish the dictionary
                    byte[] dictMesage = new byte[dictionary.length +1];
                    dictMesage[0] = dictionaryId;
                    dictionaryId = (byte)((dictionaryId+1) % 127); //calculate the next dictId
//                    for(int i = 0; i < dictionary.length; i++){ //copy dictionary content to dictMessage
//                        dictMesage[i+1] = dictionary[i];
//                    }
                    System.arraycopy(dictionary, 0, dictMesage, 1, dictionary.length);


//                    MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false,
//                            MqttQoS.AT_LEAST_ONCE, false, 0);
//                    MqttPublishVariableHeader variableHeader = new MqttPublishVariableHeader("Const.DICT_TOPIC_NAME",000)
                    //TODO why do we need this code block of sending a byte with -103
//                    byte[] cmdb = new byte[1];
//                    cmdb[0] = -103;
//                    PublishMessage cmd = new PublishMessage();
//                    cmd.setTopicName(Const.DICT_TOPIC_NAME);
//                    cmd.setPayload(ByteBuffer.wrap(cmdb));
//                    cmd.setQos(AbstractMessage.QOSType.LEAST_ONE);
//                    this.mqttBroker.internalPublish(cmd);
//                    this.mqttBroker.internalPublish(cmd);
//                    this.mqttBroker.internalPublish(cmd);
//                    this.mqttBroker.internalPublish(cmd);
//                    this.mqttBroker.internalPublish(cmd);
//                    this.mqttBroker.internalPublish(cmd);
//                    this.mqttBroker.internalPublish(cmd);


                    //Publish the new dictionry to clients
                    PublishMessage pm = new PublishMessage();
                    pm.setTopicName(Const.DICT_TOPIC_NAME);
                    pm.setPayload(ByteBuffer.wrap(dictMesage));
                    pm.setQos(AbstractMessage.QOSType.LEAST_ONE);
                    this.mqttBroker.internalPublish(pm);

                    System.out.println("################ Finished sampling dictionary ##################");

                } catch (IOException e) {
                    e.printStackTrace();
                }
                createDictionary = false;
            }

        }
    }

    @Override
    public void onSubscribe(InterceptSubscribeMessage msg) {
        System.out.println("broker: #onsubscribe: " + msg.getTopicFilter() + " #clientID:" + new String(msg.getClientID()));
    }

    public void onUnsubscribe(InterceptUnsubscribeMessage msg) {
        System.out.println("TODO");
    }
}
