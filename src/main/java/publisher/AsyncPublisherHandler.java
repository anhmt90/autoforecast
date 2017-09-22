//import org.apache.commons.io.LineIterator;
//import org.eclipse.paho.client.mqttv3.*;
//import org.toubassi.femtozip.models.FemtoZipCompressionModel;
//
//import java.util.Arrays;
//import java.util.Dictionary;
//import java.util.Enumeration;
//import java.util.Hashtable;
//
///**
// * Created by Tanuj on 3/3/2016.
// */
//public class AsyncPublisherHandler implements MqttCallback, Runnable  {
//
//    private final MqttAsyncClient cl;
//    private final LineIterator it;
//    private final Dictionary<Byte, FemtoZipCompressionModel> dictionaries;
//    private boolean hasSentBeginCompressionNotification;
//    private boolean hasSentEndCompressionNotification;
//
//    private int compressedCnt;
//    private boolean hasSentBeginUnCompressionNotification;
//    private boolean hasSentEndUnCompressionNotification;
//    private int uncompressedCnt;
//
//
//    public AsyncPublisherHandler(MqttAsyncClient cl, LineIterator it)  {
//        hasSentBeginCompressionNotification = false;
//        hasSentEndCompressionNotification = false;
//        hasSentBeginUnCompressionNotification = false;
//        hasSentEndUnCompressionNotification = false;
//        this.cl = cl;
//        this.it = it;
//        dictionaries = new Hashtable<>();
//        compressedCnt = 0;
//        uncompressedCnt = 0;
//    }
//
//    public void subscribe() throws MqttException {
//        this.cl.subscribe(Const.DICT_TOPIC_NAME, 1);
//    }
//
//    public void startSendingMessages() throws Exception {
//
//        byte[] ttemp = new byte[1];
//        ttemp[0] = -99;
//        this.cl.publish(Const.TOPIC_NAME, new MqttMessage(ttemp));
//
//
//        while(it.hasNext()) {
//            System.out.println("#cnt:" + uncompressedCnt);
//
//            String next = (String)it.next();
//            boolean empty = false;
//            synchronized (dictionaries) {
//                empty = dictionaries.isEmpty();
//            }
//
//            byte[] payload = next.getBytes();
//
//
//            if(empty && !hasSentEndUnCompressionNotification) {
//                uncompressedCnt++;
//                if(!hasSentBeginUnCompressionNotification) {
//                    byte[] temp = new byte[1];
//                    temp[0] = -99;
//                    this.cl.publish(Const.TOPIC_NAME, new MqttMessage(temp));
//                    hasSentBeginUnCompressionNotification = true;
//                    uncompressedCnt = 0;
//                    System.out.println("#meta:-99");
//                }
//                if(!hasSentEndUnCompressionNotification && (uncompressedCnt == 5000)){
//                    byte[] temp = new byte[1];
//                    temp[0] = -100;
//                    this.cl.publish(Const.TOPIC_NAME, new MqttMessage(temp));
//                    hasSentEndUnCompressionNotification = true;
//                    System.out.println("#meta:-100");
//                }
//
//                byte[] msg = new byte[payload.length + 1];
//                msg[0] = -1;
//
//                this.cl.publish(Const.TOPIC_NAME, new MqttMessage(msg));
//            }
//            else if(!empty && !hasSentEndCompressionNotification) {
//                if(!hasSentBeginCompressionNotification) {
//                    byte[] temp = new byte[1];
//                    temp[0] = -101;
//                    this.cl.publish(Const.TOPIC_NAME, new MqttMessage(temp));
//                    hasSentBeginCompressionNotification = true;
//                    compressedCnt = 0;
//                    System.out.println("#meta:-101");
//                }
//                if(!hasSentEndCompressionNotification && (compressedCnt == 5000)){
//                    byte[] temp = new byte[1];
//                    temp[0] = -102;
//                    this.cl.publish(Const.TOPIC_NAME, new MqttMessage(temp));
//                    hasSentEndCompressionNotification = true;
//                    System.out.println("#meta:-102");
//                }
//
//
//                byte b = latestDict();
//                FemtoZipCompressionModel femtoZipCompressionModel;
//
//                synchronized (dictionaries) {
//                    femtoZipCompressionModel = dictionaries.get(b);
//                }
//                byte[] compressedMessage = femtoZipCompressionModel.compress(payload);
//                byte[] msg = new byte[compressedMessage.length+1];
//                msg[0] = b;
//                for(int i = 0; i < compressedMessage.length; i++){
//                    msg[i+1] = compressedMessage[i];
//                }
//
//                System.out.println("#published:" + msg.length);
//                this.cl.publish(Const.TOPIC_NAME, new MqttMessage(msg));
//
//                compressedCnt++;
//            }
//            else {
//                System.out.println("waiting for dict or finished");
//                Thread.sleep(1000);
//            }
//
//
//        }
//    }
//
//
//    private byte latestDict() {
//        byte smallest = 0;
//        synchronized (dictionaries) {
//            Enumeration<Byte> keys = dictionaries.keys();
//
//            while (keys.hasMoreElements()) {
//                Byte aByte = keys.nextElement();
//                if (aByte > smallest) {
//                    smallest = aByte;
//                }
//            }
//        }
//        return smallest;
//    }
//
//
//
//    @Override
//    public void connectionLost(Throwable throwable) {
//        System.out.println("Publisherhandler-connectionLost");
//        throwable.printStackTrace();
//    }
//
//    @Override
//    public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
//
//        byte[] msgPayload = mqttMessage.getPayload();
//        byte id = msgPayload[0];
//        if(id < -90) {
//            return;
//        }
//        byte[] payload = Arrays.copyOfRange(msgPayload, 1, msgPayload.length);
//
//        if(s.equalsIgnoreCase(Const.DICT_TOPIC_NAME)) {
//            System.out.println("#got-dictionary:" + payload.length + "#cnt:" + compressedCnt);
//            FemtoZipCompressionModel femtoZipCompressionModel1 = FemtoFactory.fromDictionary(payload);
//            dictionaries.put(id, femtoZipCompressionModel1);
//        }
//        if(s.equalsIgnoreCase(Const.TOPIC_NAME)) {
//            if(id == -1) {
//                System.out.println("#uncompressedMessage:" + payload.length);
//            }
//            else if (id > 0) {
//                FemtoZipCompressionModel femtoZipCompressionModel = dictionaries.get(id);
//                byte[] uncompressedMessage = femtoZipCompressionModel.decompress(payload);
//                System.out.println("#compressedMessage:" + uncompressedMessage.length);
//            }
//        }
//    }
//
//    @Override
//    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
//
//    }
//
//    @Override
//    public void run() {
//        try {
//            startSendingMessages();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//}
