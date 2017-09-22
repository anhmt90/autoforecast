//import org.rosuda.REngine.REXP;
//import org.rosuda.REngine.REXPMismatchException;
//import org.rosuda.REngine.REngineException;
//import org.rosuda.REngine.Rserve.RConnection;
//import org.rosuda.REngine.Rserve.RserveException;
//
//public class Prediction {
//
//    public static void main(String[] args) {
//        String forecastScript = System.getProperty("user.dir")+"/src/main/resources/forRserve.R"; ;
//        RConnection connection = null;
//
//        try {
//             /* Create a connection to Rserve instance running
//              * on default port 6311
//              */
//            connection = new RConnection();
//
//////          REXPString result = (REXPString) connection.parseAndEval(String.format("capture.output(source('%s'))", jriScript));
//            connection.eval(String.format("capture.output(source('%s'))", forecastScript));
//            REXP res = connection.eval("res$mean");
//            //REXPString result =  connection.parseAndEval(String.format("capture.output(source('%s'))", jriScript));
//
////            RList output = res.asList();
////            org.rosuda.REngine.REXPGenericVector@420095a8+[19]named
////            org.rosuda.REngine.REXPDouble@424c38b2+[72]
////            org.rosuda.REngine.REXPDouble@79a2efbd[2]
////            org.rosuda.REngine.REXPInteger@3c986fe2+[600]
////            org.rosuda.REngine.REXPDouble@7af3100c+[144]
////            org.rosuda.REngine.REXPDouble@59592e48+[144]
////            org.rosuda.REngine.REXPDouble@3768edd9+[600]
////            org.rosuda.REngine.REXPString@487073a9[1]
////            org.rosuda.REngine.REXPString@2a6169db[1]
////            org.rosuda.REngine.REXPDouble@3a5a39b1+[600]
////
////            Point Forecast    Lo 80    Hi 80    Lo 95    Hi 95
////            26.00000       4706.504 3673.310 5739.699 3126.370 6286.638
////            26.04167       5266.210 4103.919 6428.500 3488.640 7043.780
////            26.08333       5001.935 3889.178 6114.692 3300.120 6703.750
////            26.12500       4727.718 3666.600 5788.836 3104.878 6350.558
////            26.16667       5676.788 4405.401 6948.176 3732.369 7621.208
////
//            double pointForecast[] = res.asDoubles();
////          double low80[] = output.at(2).asDoubles();
//            System.out.println("The forecast is: ");
//            for (int i = 0; i < pointForecast.length; i++) {
//                System.out.println(pointForecast[i]);
//            }
//        } catch (RserveException e) {
//            e.printStackTrace();
//        } catch (REXPMismatchException e) {
//            e.printStackTrace();
//        } catch (REngineException e) {
//            e.printStackTrace();
//        } finally{
//            connection.close();
//        }
//        System.out.println(forecastScript);
//    }
//}
