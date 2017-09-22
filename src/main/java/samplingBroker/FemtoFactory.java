package samplingBroker;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.toubassi.femtozip.models.FemtoZipCompressionModel;

import java.io.*;

/**
 * Created by chris on 19.02.16.
 */
public class FemtoFactory {
    public static FemtoZipCompressionModel fromDictionary(byte[] dict) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(dict);
        DataInputStream dis = new DataInputStream(bis);

        FemtoZipCompressionModel compressionModel = new FemtoZipCompressionModel();
        compressionModel.load(dis);

        dis.close();

        return compressionModel;
    }
    /*
        @return: a dictionary as byte array
    */
    public static byte[] getDictionary(FemtoZipCompressionModel compressionModel) throws IOException {

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        compressionModel.save(dos);
        dos.flush();

        return bos.toByteArray();
    }

    public static File getDictCacheFile() {
        return new File("./dict");
    }

    public static boolean isCachedDictionaryAvailable() {
        File dictFile = getDictCacheFile();
        return dictFile.exists() && !dictFile.isDirectory();
    }

    public static byte[] fromCache() throws IOException {
        return IOUtils.toByteArray(new FileInputStream(getDictCacheFile()));
    }

    public static void toCache(FemtoZipCompressionModel compressionModel) throws IOException {
        FileUtils.writeByteArrayToFile(getDictCacheFile(), getDictionary(compressionModel));
    }

}
