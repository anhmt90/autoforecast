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
import io.moquette.spi.impl.subscriptions.Subscription;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.jetbrains.annotations.Nullable;
import org.toubassi.femtozip.ArrayDocumentList;
import org.toubassi.femtozip.models.FemtoZipCompressionModel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.apache.commons.lang3.math.NumberUtils.max;

/**
 * Created by chris on 19.02.16.
 */
public class SamplingBrokerHandler extends AbstractInterceptHandler {

    private class SampleMessage{
        long timestamp;
        byte[] payload;
        SampleMessage(long timestamp, byte[] payload){
            this.timestamp = timestamp;
            this.payload = payload;
        }
    }

    private final Server mqttBroker;
    private final CircularFifoQueue<SampleMessage> samplingQueue;  //used to sample content for shared dict
//    private long startTimestampOfSamplingQueue;
//    private long endTimestampOfSamplingQueue;
    private final Dictionary<Byte, FemtoZipCompressionModel> dictionaries;
    private long uncompressedCnt;
    private long compressedCnt;
    private byte dictionaryId;
    private boolean createNewDictionary;

    public SamplingBrokerHandler(Server mqttBroker) {

        this.mqttBroker = mqttBroker;
        this.samplingQueue = new CircularFifoQueue<>(120);
        dictionaries = new Hashtable<>();
        uncompressedCnt = 1;
        compressedCnt = 1;
        dictionaryId = 0;
        createNewDictionary = false;
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


            if(header == -1) { //-1: uncompressed messages
                samplingQueue.add(new SampleMessage(System.currentTimeMillis()/1000, payload));
                System.out.println("#" +  uncompressedCnt + " UNCOMP - FORW:" + msg.getPayload().array().length + "bytes - TOPIC:" + msg.getTopicName());
                ++uncompressedCnt;
            }
            else if(header >= 0) { //if positive, it represents a new dictionary ID dictionaryId
                System.out.println("#" +  compressedCnt + " COMP - FORW:" + msg.getPayload().array().length + "bytes - TOPIC:" + msg.getTopicName());
                ++compressedCnt;
            }

            byte[] dictionary = new byte[0];
            if((uncompressedCnt == samplingQueue.maxSize() && !FemtoFactory.isCachedDictionaryAvailable())){
                //TODO add more if condition when FemtoFactory.isCachedDictionaryAvailable() = true and predict msg rate
                System.out.println("#Creating a new dictionary .................");
                FemtoZipCompressionModel femtoZipCompressionModel = new FemtoZipCompressionModel();
                try{
                    System.out.println("#Building a new dictionary based on the last " + uncompressedCnt + " uncompressed messages");
                    //Build the new dictionary
                    femtoZipCompressionModel.setMaxDictionaryLength(calcDictionarySize()); //Todo make it better
                    femtoZipCompressionModel.build(new ArrayDocumentList(getAllPayloadOfSamplingQueue()));
                    System.out.println("#Dictionary built");

                    //Caching the sampled dictionary
                    FemtoFactory.toCache(femtoZipCompressionModel);

                    //get the sampled dict
                    dictionary = FemtoFactory.getDictionary(femtoZipCompressionModel);
                    publishNewDictionary(Const.DICT_TOPIC_NAME, dictionary);
                    System.out.println("#Dictionary published");
                    uncompressedCnt = 1; //reset unCommpressCnt
                    requireNewDictionary();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
//            else if (FemtoFactory.isCachedDictionaryAvailable()){
//            }


        }
    }

    @Override
    public void onSubscribe(InterceptSubscribeMessage msg) {
        System.out.println("broker: #onsubscribe: " + msg.getTopicFilter() + " #clientID:" + msg.getClientID());
        if(FemtoFactory.isCachedDictionaryAvailable() && msg.getClientID().equalsIgnoreCase(msg.getTopicFilter())){
            try {
                publishCachedDictionary(msg.getClientID(), FemtoFactory.fromCache());
            } catch (IOException e) {
            }
        }
    }

    public void onUnsubscribe(InterceptUnsubscribeMessage msg) {
        System.out.println("#"+msg.getClientID()+" UNSUB - TOPIC" + msg.getTopicFilter());
    }

    /*Publish the cached dictionary when new clients subscribe*/
    private void publishCachedDictionary(String topic, byte[] dictionary) {
        byte[] sharedDict = new byte[dictionary.length +1];
        sharedDict[0] = dictionaryId;
        System.arraycopy(dictionary, 0, sharedDict, 1, dictionary.length);

        PublishMessage pm = new PublishMessage();
        pm.setTopicName(topic);
        pm.setPayload(ByteBuffer.wrap(sharedDict));
        pm.setQos(AbstractMessage.QOSType.LEAST_ONE);

        this.mqttBroker.internalPublish(pm);
    }

    /*Publish a new dictionary by sampling*/
    private void publishNewDictionary(String topic, byte[] dictionary) {
        byte[] sharedDict = new byte[dictionary.length +1];
        dictionaryId = (byte) ((dictionaryId + 1) % 127); //calculate the next dictId
        sharedDict[0] = dictionaryId;
        System.arraycopy(dictionary, 0, sharedDict, 1, dictionary.length);

        PublishMessage pm = new PublishMessage();
        pm.setTopicName(Const.DICT_TOPIC_NAME);
        pm.setPayload(ByteBuffer.wrap(sharedDict));
        pm.setQos(AbstractMessage.QOSType.LEAST_ONE);

        this.mqttBroker.internalPublish(pm);
    }





    private boolean requireNewDictionary() throws IOException {
        ArrayList<byte[]> N = getAllPayloadOfSamplingQueue();

        int splitPos = (int)(samplingQueue.size()*0.7);
        ArrayList<byte[]> N_train = new ArrayList<>(N.subList(0,splitPos));
        ArrayList<byte[]> N_test = new ArrayList<>(N.subList(splitPos+1, N.size()-1));

        FemtoZipCompressionModel femtoZipCompressionModel = new FemtoZipCompressionModel();
        femtoZipCompressionModel.build(new ArrayDocumentList(N_train));

        int TB_test = 0;
        for(byte[] testPayload : N_test)
            TB_test += testPayload.length;

        int CTB_test = 0;
        for(byte[] testPayload : N_test)
            CTB_test += femtoZipCompressionModel.compress(testPayload).length;


        long bandwidthReduction = 1 - (CTB_test/TB_test);

        long totalSize = 0;
        for(byte[] testPayload : N){
            totalSize = totalSize + testPayload.length;
        }
        System.out.println("Average message size = " + (totalSize/N.size()) );
        long messageRate = totalSize/getTimeSpanOfSamplingQueue();
        System.out.println("Message rate = " + messageRate );

        long T_amortize = femtoZipCompressionModel.getDictionary().length/(bandwidthReduction * messageRate);
        System.out.println("|SD| = " + femtoZipCompressionModel.getDictionary().length);
        System.out.println("#CONSTANT_RATE: Dictionary expires in " + (T_amortize * 10) + " seconds");

        return false;
    }






    private long getTimeSpanOfSamplingQueue(){
        if(samplingQueue.isEmpty() || samplingQueue.size() == 1){
            return 0;
        }
        return (samplingQueue.get(samplingQueue.size()-1).timestamp - samplingQueue.get(0).timestamp);
    }

    @Nullable
    private ArrayList<Long> getAllTimeStampOfSamplingQueue(){
        if(samplingQueue.isEmpty())return null;
        ArrayList<Long> timestampList = new ArrayList<>(samplingQueue.size());
        for (SampleMessage sm : samplingQueue){
            timestampList.add(sm.timestamp);
        }
        return timestampList;
    }

    @Nullable
    private ArrayList<byte[]> getAllPayloadOfSamplingQueue(){
        if(samplingQueue.isEmpty()) return null;
        ArrayList<byte[]> payloadList = new ArrayList<>(samplingQueue.size());
        for (SampleMessage sm : samplingQueue){
            payloadList.add(sm.payload);
        }
        return payloadList;
    }

}
