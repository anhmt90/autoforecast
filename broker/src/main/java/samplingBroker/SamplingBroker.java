package samplingBroker;

import io.moquette.BrokerConstants;
import io.moquette.interception.InterceptHandler;
import io.moquette.server.Server;
import io.moquette.server.config.IConfig;
import io.moquette.server.config.MemoryConfig;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static java.util.Arrays.asList;
/**
 * Created by chris on 19.02.16.
 */


public class SamplingBroker {
    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("<<<<<<<<<<<<<<<<<<<< Starting SB >>>>>>>>>>>>>>>>>>>>");
        System.out.println("Topic: " + Const.TOPIC_NAME);
        Properties m_properties = new Properties();
        m_properties.put(BrokerConstants.PORT_PROPERTY_NAME, Integer.toString(BrokerConstants.PORT));
        m_properties.put(BrokerConstants.HOST_PROPERTY_NAME, BrokerConstants.HOST);
        m_properties.put(BrokerConstants.WEB_SOCKET_PORT_PROPERTY_NAME, Integer.toString(BrokerConstants.WEBSOCKET_PORT));
        m_properties.put(BrokerConstants.PASSWORD_FILE_PROPERTY_NAME, "");
        m_properties.put(BrokerConstants.PERSISTENT_STORE_PROPERTY_NAME, BrokerConstants.DEFAULT_PERSISTENT_PATH);
        m_properties.put(BrokerConstants.ALLOW_ANONYMOUS_PROPERTY_NAME, true);
        m_properties.put(BrokerConstants.AUTHENTICATOR_CLASS_NAME, "");
        m_properties.put(BrokerConstants.AUTHORIZATOR_CLASS_NAME, "");

        IConfig inMemoryConfig = new MemoryConfig(m_properties);

        Server mqttBroker = new Server();
        SamplingBrokerHandler sb = new SamplingBrokerHandler(mqttBroker);
        List<? extends InterceptHandler> userHandlers = asList(sb);
        mqttBroker.startServer(inMemoryConfig, userHandlers);


        System.out.println("SamplingBroker started press [CTRL+C] to stop");
        //Bind  a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("Stopping broker");
                mqttBroker.stopServer();
                System.out.println("SamplingBroker stopped");
            }
        });

        Thread.sleep(Long.MAX_VALUE);

        System.out.println("Quit");
    }

//    @Override
//    public void run() {
//        try {
//            this.main(new String[] {});
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
}
