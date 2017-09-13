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

    public static FemtoZipCompressionModel fromCache() throws IOException {
        DataInputStream dis = new DataInputStream(new FileInputStream(getDictCacheFile().getAbsolutePath()));

        FemtoZipCompressionModel compressionModel = new FemtoZipCompressionModel();
        compressionModel.load(dis);

        return compressionModel;
    }

    public static void toCache(FemtoZipCompressionModel compressionModel) throws IOException {
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(getDictCacheFile().getAbsolutePath()));

        compressionModel.save(dos);
        dos.flush();
        dos.close();
    }
}
