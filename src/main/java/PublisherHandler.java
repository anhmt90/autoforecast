import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.io.LineIterator;
import org.eclipse.paho.client.mqttv3.*;
import org.toubassi.femtozip.models.FemtoZipCompressionModel;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by chris on 19.02.16.
 */
public class PublisherHandler implements MqttCallback, Runnable {

    private final MqttClient cl;
    private final LineIterator it;
    private final Dictionary<Byte, FemtoZipCompressionModel> dictionaries;
    private boolean hasSentBeginCompressionNotification;
    private boolean hasSentEndCompressionNotification;

    private int compressedCnt;
    private boolean hasSentBeginUnCompressionNotification;
    private boolean hasSentEndUnCompressionNotification;
    private int uncompressedCnt;

    private int msgCount;

    private Map<Integer, Long> msgSentTimestamps;
    private Map<Integer, Long> msgAckTimestamps;
    
    private BufferedWriter df; //date format


    public PublisherHandler(MqttClient cl, LineIterator it) throws IOException {
        hasSentBeginCompressionNotification = false;
        hasSentEndCompressionNotification = false;
        hasSentBeginUnCompressionNotification = false;
        hasSentEndUnCompressionNotification = false;
        this.cl = cl;
        this.it = it;
        dictionaries = new Hashtable<>();
        compressedCnt = 0;
        uncompressedCnt = 0;
        msgCount = 0;
        msgSentTimestamps = new HashedMap<>();
        msgAckTimestamps = new HashedMap<>();

        SimpleDateFormat sdf = new SimpleDateFormat("HH_mm");
        Date resultdate = new Date(System.currentTimeMillis());

        File resultFolder = new File("./sdc_latency/");

        if (!resultFolder.exists())
            resultFolder.mkdir();

        File resultFile = new File(resultFolder, "result_" + sdf.format(resultdate) + ".csv");

        df = new BufferedWriter(new FileWriter(resultFile, false));
        df.write("msgNo, latency, uncommpressed/compressed");
        df.newLine();
        df.flush();
    }

    public void subscribe() throws MqttException {
        this.cl.subscribe(Const.DICT_TOPIC_NAME);
        this.cl.subscribe(Const.REPLY_TOPIC_NAME);
    }

    public void startSendingMessages() throws Exception {
        byte[] ttemp = new byte[1];
        ttemp[0] = -99;
        this.cl.publish(Const.TOPIC_NAME, new MqttMessage(ttemp));


        while(it.hasNext()) {
            System.out.println("#cnt:" + uncompressedCnt);

            msgCount++;

            String next = (String)it.next();
            boolean empty = false;
            synchronized (dictionaries) {
                empty = dictionaries.isEmpty();
            }

            byte[] payload = next.getBytes();


            if(empty && !hasSentEndUnCompressionNotification) {
                uncompressedCnt++;
                if(!hasSentBeginUnCompressionNotification) {
                    System.out.println("Sending uncompressed messages");
                    byte[] temp = new byte[1];
                    temp[0] = -99;
                    this.cl.publish(Const.TOPIC_NAME, new MqttMessage(temp));
                    hasSentBeginUnCompressionNotification = true;
                    uncompressedCnt = 0;
                    System.out.println("#meta:-99");
                }
                if(!hasSentEndUnCompressionNotification && (uncompressedCnt == 10)){
                    byte[] temp = new byte[1];
                    temp[0] = -100;
                    this.cl.publish(Const.TOPIC_NAME, new MqttMessage(temp));
                    hasSentEndUnCompressionNotification = true;
                    System.out.println("#meta:-100");

                    calculateLatency("uncompressed");
                    System.out.println("All uncompressed messages sent");
                }

                byte[] msg = new byte[payload.length + 5];
                msg[0] = -1;
                System.arraycopy(ByteBuffer.allocate(4).putInt(msgCount).array(), 0, msg, 1, 4);
                System.arraycopy(payload, 0, msg, 5, payload.length);

                this.cl.publish(Const.TOPIC_NAME, new MqttMessage(msg));
                msgSentTimestamps.put(msgCount, System.nanoTime());
            }
            else if(!empty && !hasSentEndCompressionNotification) {
                if(!hasSentBeginCompressionNotification) {
                    System.out.println("Sending compressed messages");
                    byte[] temp = new byte[1];
                    temp[0] = -101;
                    this.cl.publish(Const.TOPIC_NAME, new MqttMessage(temp));
                    hasSentBeginCompressionNotification = true;
                    compressedCnt = 0;
                    System.out.println("#meta:-101");
                }
                if(!hasSentEndCompressionNotification && (compressedCnt == 10)){
                    byte[] temp = new byte[1];
                    temp[0] = -102;
                    this.cl.publish(Const.TOPIC_NAME, new MqttMessage(temp));
                    hasSentEndCompressionNotification = true;
                    System.out.println("#meta:-102");

                    calculateLatency("compressed");
                    System.out.println("All compressed messages sent");
                }


                byte b = latestDict();
                FemtoZipCompressionModel femtoZipCompressionModel;

                synchronized (dictionaries) {
                    femtoZipCompressionModel = dictionaries.get(b);
                }
                byte[] compressedMessage = femtoZipCompressionModel.compress(payload);
                byte[] msg = new byte[compressedMessage.length + 5];

                msg[0] = b;
                System.arraycopy(ByteBuffer.allocate(4).putInt(msgCount).array(), 0, msg, 1, 4);
                System.arraycopy(compressedMessage, 0, msg, 5, compressedMessage.length);

                System.out.println("#published:" + msg.length);
                this.cl.publish(Const.TOPIC_NAME, new MqttMessage(msg));
                msgSentTimestamps.put(msgCount, System.nanoTime());

                compressedCnt++;
            } else {
                System.out.println("waiting for dict or finished");
                Thread.sleep(1000);
            }
        }
    }

    private void calculateLatency(String msgType) throws IOException {
        Map<Integer, Long> latencyMap = new HashMap<>();

        double avgLatency = 0;
        int size = latencyMap.size();

        Iterator it = msgAckTimestamps.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            System.out.println(pair.getKey() + " = " + pair.getValue());

            int key = (int) pair.getKey();
            long latency = (long) pair.getValue();

            if (msgSentTimestamps.containsKey(key)) {
                latency -= msgSentTimestamps.get(key);
                latencyMap.put(key, latency);
                avgLatency += latency / size;

                df.write(key + "," + latency + "," + msgType);
                df.newLine();
            }
        }

        df.flush();

        if (msgType.equalsIgnoreCase("compressed"))
            df.close();


//        avgLatency /= size;
        System.out.println("Average Latency : " + avgLatency);
        System.out.println("No of msgs : " + size);

        msgSentTimestamps = new HashedMap<>();
        msgAckTimestamps = new HashedMap<>();
    }


    private byte latestDict() {
        byte smallest = 0;
        synchronized (dictionaries) {
            Enumeration<Byte> keys = dictionaries.keys();

            while (keys.hasMoreElements()) {
                Byte aByte = keys.nextElement();
                if (aByte > smallest) {
                    smallest = aByte;
                }
            }
        }
        return smallest;
    }



    @Override
    public void connectionLost(Throwable throwable) {
        System.out.println("Publisherhandler-connectionLost");
        throwable.printStackTrace();
    }

    @Override
    public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
        byte[] msgPayload = mqttMessage.getPayload();
        byte id = msgPayload[0];
        if(id < -90) {
            return;
        }
        byte[] payload = Arrays.copyOfRange(msgPayload, 1, msgPayload.length);

        if(s.equalsIgnoreCase(Const.DICT_TOPIC_NAME)) {
            System.out.println("#got-dictionary:" + payload.length + "#cnt:" + compressedCnt);
            FemtoZipCompressionModel femtoZipCompressionModel1 = FemtoFactory.fromDictionary(payload);
            dictionaries.put(id, femtoZipCompressionModel1);
        }
        if(s.equalsIgnoreCase(Const.TOPIC_NAME)) {
            if(id == -1) {
                System.out.println("#uncompressedMessage:" + payload.length);
            } else if (id > 0) {
                FemtoZipCompressionModel femtoZipCompressionModel = dictionaries.get(id);
                byte[] uncompressedMessage = femtoZipCompressionModel.decompress(payload);
                System.out.println("#compressedMessage:" + uncompressedMessage.length);
            }
        }
        if (s.equalsIgnoreCase(Const.REPLY_TOPIC_NAME)) {
            long currentTime = System.nanoTime();
            int msgNo = ByteBuffer.wrap(Arrays.copyOfRange(payload, 0, 4)).getInt();
            System.out.println("Msg No : " + msgNo);
            msgAckTimestamps.put(msgNo, currentTime);
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

    }

    @Override
    public void run() {
        try {
            startSendingMessages();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
