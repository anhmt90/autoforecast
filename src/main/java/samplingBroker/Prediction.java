//package samplingBroker;
//
//import org.rosuda.REngine.REXP;
//import org.rosuda.REngine.REXPMismatchException;
//import org.rosuda.REngine.REngineException;
//import org.rosuda.REngine.Rserve.RConnection;
//import org.rosuda.REngine.Rserve.RserveException;
//
//public class Prediction {
//
//    public static void main(String[] args) {
//        String forecastScript = System.getProperty("user.dir")+"/src/main/resources/forecastExpiry.R"; ;
//        RConnection connection = null;
//        try {
//             /* Create a connection to Rserve instance running
//              * on default port 6311
//              */
//            connection = new RConnection();
//
//            connection.eval(String.format("source('%s')", forecastScript));
//            REXP res = connection.eval("result$mean");
//
//            int pointForecast[] = res.asIntegers();
//            int sum = 0;
//            int expiredMin = 0;
//            int expiryAssumption = 10000; //messages
//            System.out.println("The forecast is: ");
//            for (int i = 0; i < pointForecast.length; i++) {
//                System.out.println(pointForecast[i]);
//            }
//
//            for (int i = 0; i < pointForecast.length - 1; i++) {
//                if((sum + pointForecast[i]) < expiryAssumption){
//                    sum += pointForecast[i];
//                    expiredMin += 60;
//                } else {
//                    expiredMin += (expiryAssumption - sum)*60/pointForecast[i];
//                    break;
//                }
//            }
//            System.out.println(">>>>>>>>>>>>>>>>>> FORECAST: Dictionary expires in " + expiredMin + "minutes");
//
//        } catch (RserveException e) {
//            e.printStackTrace();
//        } catch (REXPMismatchException e) {
//            e.printStackTrace();
//        } finally{
//            connection.close();
//        }
//        System.out.println(forecastScript);
//    }
//}
