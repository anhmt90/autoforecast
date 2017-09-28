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
import org.jetbrains.annotations.Nullable;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;
import org.toubassi.femtozip.ArrayDocumentList;
import org.toubassi.femtozip.models.FemtoZipCompressionModel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.apache.commons.lang3.math.NumberUtils.max;
import static org.apache.commons.lang3.math.NumberUtils.min;

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
    private final CircularFifoQueue<SampleMessage> samplingQueueRealTime;  //used to sample content for shared dict
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
        this.samplingQueueRealTime = new CircularFifoQueue<>(100);
        this.samplingQueue = new CircularFifoQueue<>(1000);
        dictionaries = new Hashtable<>();
        uncompressedCnt = 0;
        compressedCnt = 0;
        dictionaryId = 0;
        createNewDictionary = false;
    }

//    private int calcDictionarySize() {
//        return 190; //todo make better
//    }

    @Override
    public void onPublish(InterceptPublishMessage msg) {
        if(msg.getTopicName().equalsIgnoreCase(Const.TOPIC_NAME)) {
            byte[] received = msg.getPayload().array();
            byte header = received[0];
            int timestamp = msg.getPayload().getInt(1);
            byte[] payload = new byte[received.length-5];
            System.arraycopy(received, 5, payload, 0, received.length-5);

            if(header == -1) { //-1: uncompressed messages
                samplingQueue.add(new SampleMessage(timestamp, payload));
                ++uncompressedCnt;
                System.out.println("#" +  uncompressedCnt + " UNCOMP - FORW:" + msg.getPayload().array().length + "bytes - TOPIC:" + msg.getTopicName());
                System.out.println(timestamp+":: " + (new String(payload)) + "\n");
            }
            else if(header >= 0) { //if positive, it represents a new dictionary ID dictionaryId
                ++compressedCnt;
                System.out.println("#" +  compressedCnt + " COMP - FORW:" + msg.getPayload().array().length + " bytes - TOPIC:" + msg.getTopicName());

            }
//            System.out.println(timestamp);
//            System.out.println(new String(payload));
            byte[] dictionary = new byte[0];
            if((uncompressedCnt == samplingQueue.maxSize() && !FemtoFactory.isCachedDictionaryAvailable())){
                //TODO add more if condition when FemtoFactory.isCachedDictionaryAvailable() = true and predict msg rate
                System.out.println("#Creating a new dictionary .................");
                FemtoZipCompressionModel femtoZipCompressionModel = new FemtoZipCompressionModel();
                try{
                    System.out.println("#Building a new dictionary based on the last " + uncompressedCnt + " uncompressed messages");
                    //Build the new dictionary
                    femtoZipCompressionModel.setMaxDictionaryLength(2000000); //Todo make it better
                    femtoZipCompressionModel.build(new ArrayDocumentList(getAllPayloadOfSamplingQueue(samplingQueue)));
                    System.out.println("#Dictionary built");

                    //Caching the sampled dictionary
                    FemtoFactory.toCache(femtoZipCompressionModel);

                    //get the sampled dict
                    dictionary = FemtoFactory.getDictionary(femtoZipCompressionModel);
                    publishNewDictionary(Const.DICT_TOPIC_NAME, dictionary);
                    System.out.println("#Dictionary published");
                    uncompressedCnt = 1; //reset unCommpressCnt
                    calcExpiry(samplingQueue);
                    Thread.sleep(60000);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            else if (FemtoFactory.isCachedDictionaryAvailable()){
            }


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





    private void calcExpiry(CircularFifoQueue<SampleMessage> cfq) throws IOException {
        ArrayList<byte[]> N = getAllPayloadOfSamplingQueue(cfq);

        int splitPos = (int)(cfq.size()*0.7);
        ArrayList<byte[]> N_train = new ArrayList<>(N.subList(0,splitPos));
        ArrayList<byte[]> N_test = new ArrayList<>(N.subList(splitPos+1, N.size()));

        FemtoZipCompressionModel femtoZipCompressionModel = new FemtoZipCompressionModel();
        femtoZipCompressionModel.build(new ArrayDocumentList(N_train));

        /*<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< Calculate by CONSTANT RATE >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>*/

        int TB_test = 0;
        for(byte[] testPayload : N_test)
            TB_test += testPayload.length;

        int CTB_test = 0;
        for(byte[] testPayload : N_test)
            CTB_test += femtoZipCompressionModel.compress(testPayload).length;

        long totalSize = 0;
        for(byte[] testPayload : N){
            totalSize = totalSize + testPayload.length;
        }

        long bandwidthReduction = 1 - (CTB_test/TB_test);
        double avgMessageSize = totalSize/N.size();
        System.out.println("Average message size = " + avgMessageSize + " bytes" );

        long rate = totalSize/getTimeSpanOfSamplingQueue(cfq);
        System.out.println("Rate = " + rate + " bytes/second");

        int SD = femtoZipCompressionModel.getDictionary().length;
        System.out.println("|SD| = " + SD + " bytes");

        long T1_amortize = SD/(rate * bandwidthReduction);
        System.out.println(">>>>>>>>>>>>>>>>>> CONSTANT RATE: Dictionary expires in " + (T1_amortize * 10) + " seconds");

        /*<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< Calculate by FORECAST RATES >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>*/
        String forecastScript = System.getProperty("user.dir")+"/broker/src/main/resources/forecastExpiry.R"; ;
        System.out.println("forecastScript = "+forecastScript);
        RConnection connection = null;
        try {
             /* Create a connection to Rserve instance running on default port 6311 */
            connection = new RConnection();
            connection.eval(String.format("source('%s')", forecastScript));
            REXP res = connection.eval("result$mean");

            double pointForecast[] = res.asDoubles(); //messages per hour

            System.out.println("Point forecasts (messages per hour): ");
            for (int i = 0; i < pointForecast.length; i++) {
                System.out.println((i+1) + ". " + pointForecast[i]);
            }
//            int sum = 0;
//            int expiredMin = 0;
//            int expiryAssumption = 10000; //messages
//            for (int i = 0; i < pointForecast.length - 1; i++) {
//                if((sum + pointForecast[i]) < expiryAssumption){
//                    sum += pointForecast[i];
//                    expiredMin += 60;
//                } else {
//                    expiredMin += (expiryAssumption - sum)*60/pointForecast[i];
//                    break;
//                }
//            }
            double T2_amortize = 0;
            double SDRest = SD;
            for (int i = 0; i < pointForecast.length; i++) {
                double ratePerSecond =  (pointForecast[i] * avgMessageSize)/(3600); //bytes per second of the point forecast
                T2_amortize += min((SDRest/(ratePerSecond * bandwidthReduction)), 3600);
                SDRest = SDRest - (3600*ratePerSecond*bandwidthReduction);

                if(SDRest <= 0)
                    break;
            }
            System.out.println(">>>>>>>>>>>>>>>>>> FORECAST RATES: Dictionary expires in " + (T2_amortize * 10) + " seconds");

        } catch (REXPMismatchException e) {
            e.printStackTrace();
        } catch (RserveException e) {
            e.printStackTrace();
        }
    }


//    [1] 4706.890 5120.359 5391.677 4776.739 5814.571 6182.879 5971.597 6065.735 6188.243 6270.939 6025.006 6602.652
//    [13] 6232.642 5861.955 5319.898 5402.277 5074.053 4898.352 4541.940 4237.183 4251.528 4403.138 4230.262 4598.959
//    [25] 4706.890 5120.359 5391.677 4776.739 5814.571 6182.879 5971.597 6065.735 6188.243 6270.939 6025.006 6602.652
//    [37] 6232.642 5861.955 5319.898 5402.277 5074.053 4898.352 4541.940 4237.183 4251.528 4403.138 4230.262 4598.959
//    [49] 4706.890 5120.359 5391.677 4776.739 5814.571 6182.879 5971.597 6065.735 6188.243 6270.939 6025.006 6602.652
//    [61] 6232.642 5861.955 5319.898 5402.277 5074.053 4898.352 4541.940 4237.183 4251.528 4403.138 4230.262 4598.959
//    [73] 4706.890 5120.359 5391.677 4776.739 5814.571 6182.879 5971.597 6065.735 6188.243 6270.939 6025.006 6602.652
//    [85] 6232.642 5861.955 5319.898 5402.277 5074.053 4898.352 4541.940 4237.183 4251.528 4403.138 4230.262 4598.959

    private long getTimeSpanOfSamplingQueue(CircularFifoQueue<SampleMessage> cfq){
        if(cfq.isEmpty() || cfq.size() == 1){
            return 0;
        }
        return (cfq.get(cfq.size()-1).timestamp - cfq.get(0).timestamp);
    }

    @Nullable
    private ArrayList<Long> getAllTimeStampOfSamplingQueue(CircularFifoQueue<SampleMessage> cfq){
        if(cfq.isEmpty())return null;
        ArrayList<Long> timestampList = new ArrayList<>(cfq.size());
        for (SampleMessage sm : cfq){
            timestampList.add(sm.timestamp);
        }
        return timestampList;
    }

    @Nullable
    private ArrayList<byte[]> getAllPayloadOfSamplingQueue(CircularFifoQueue<SampleMessage> cfq){
        if(cfq.isEmpty()) return null;
        ArrayList<byte[]> payloadList = new ArrayList<>(cfq.size());
        for (SampleMessage sm : cfq){
            payloadList.add(sm.payload);
        }
        return payloadList;
    }

}