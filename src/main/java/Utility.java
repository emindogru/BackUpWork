import java.io.*;
import java.sql.Connection;

public class Utility {

    //TODO MYSQL VE MSSQL KOŞULLARI İÇİN AYRI AYRI BACKUP İŞLEMLERİNİ GERÇEKLEŞTİREN SINIF

    private static final Logger LOGGER = LoggerFactory.getLogger(BackUpUtility.class);
    public static boolean  backupMysqlDb(Connection connection, String backupFilePath, String user, String password, String url, String dbName) throws IOException {

        InputStream inputStream = null;

        try {
            inputStream = new FileInputStream("medy.yml");
            LOGGER.info("Yml dosyasını okuyor");
        } catch (FileNotFoundException e) {
            LOGGER.error("Yml dosyası okunurken hata oluştu: "+ e.getMessage());
            e.printStackTrace();
        }

        String dumpPath = ConfManager.getInstance().getConf().getDumpPath();
        ProcessBuilder processBuilder = new ProcessBuilder(
                dumpPath,
                "-u", user,
                "-p" + password,
                dbName,
                "-r", backupFilePath
        );

        processBuilder.redirectErrorStream(true);

        Process process = processBuilder.start();

        inputStream = process.getInputStream();
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        int ch;
        while ((ch = inputStreamReader.read()) != -1) {
            System.out.print((char) ch);
        }

        // Komutun hata çıktılarını okuma (isteğe bağlı)
        InputStream errorStream = process.getErrorStream();
        InputStreamReader errorStreamReader = new InputStreamReader(errorStream);
        while ((ch = errorStreamReader.read()) != -1) {
            System.err.print((char) ch);
        }

        int exitCode;
        try {
            exitCode = process.waitFor();
        } catch (InterruptedException e) {
            throw new IOException("Yedekleme işlemi tamamlanamadı.", e);
        }

        if (exitCode != 0) {
            throw new IOException("Yedekleme işlemi başarısız oldu. Hata kodu: " + exitCode);
        }


        return false;

    }

    public static boolean  backupMssqlDb(Connection connection, String backupFilePath, String user, String password, String url, String dbName) throws IOException {

        String database = ConfManager.getInstance().getConf().getDbName();

        ProcessBuilder processBuilder = new ProcessBuilder(
                "sqlcmd",
                "-S", "localhost",
                "-d", database,
                "-E",
                "-Q", "BACKUP DATABASE ["+database+"] TO DISK= '"+backupFilePath+ "'WITH INIT"
        );

        processBuilder.redirectErrorStream(true);

        Process process = processBuilder.start();

        InputStream inputStream = process.getInputStream();
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        int ch;
        while ((ch = inputStreamReader.read()) != -1) {
            System.out.print((char) ch);
        }

        // Komutun hata çıktılarını okuma
        InputStream errorStream = process.getErrorStream();
        InputStreamReader errorStreamReader = new InputStreamReader(errorStream);
        while ((ch = errorStreamReader.read()) != -1) {
            System.err.print((char) ch);
        }

        int exitCode;
        try {
            exitCode = process.waitFor();
        } catch (InterruptedException e) {
            throw new IOException("Yedekleme işlemi tamamlanamadı.", e);
        }

        if (exitCode != 0) {
            throw new IOException("Yedekleme işlemi başarısız oldu. Hata kodu: " + exitCode);
        }

        return false;
    }
}
