import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Date;

public class BackUpWork {

    if(willBackup)
    {
        System.out.println("in_willBackup");
        //TODO backup method here

        try {

            String confBckDir = ConfManager.getInstance().getConf().getConfBackup(); //TODO backup yeri
            String dbBckDirName = ConfManager.getInstance().getConf().getDBackup(); //TODO yml dosyasından cekilen backUp türü
            String dbBckPath = confBckDir + "/" + dbBckDirName + "/";

            String archiveFilePath = "";
            archiveFilePath = new File(ConfManager.getInstance().getConf().getArchive()).getAbsolutePath();

            Date minIOUploadDate = book.getMinIOUploadDate(new Date());


            String dbName = ConfManager.getInstance().getConf().getDbName();

            DataSourceFactory dataSourceFactory = serviceConfiguration.getHibernateConfiguration().getDataSourceFactory(serviceConfiguration);

            String user = dataSourceFactory.getUser();
            Object passwordObject = dataSourceFactory.getPassword();
            String url = dataSourceFactory.getUrl();
            String password = null;
            String extension = null;
            boolean isBacked = false;
            String dosyaIsmi = "";

            if (passwordObject instanceof String) {
                password = (String) passwordObject;
            } else if (passwordObject instanceof Integer) {
                password = String.valueOf((Integer) passwordObject);
            } else {
                System.out.println("Bilinmeyen password tipi");
            }

            java.nio.file.Path klasorYolPath = Paths.get(dbBckPath);

            try {
                Files.createDirectories(klasorYolPath);
                LOGGER.info("Klasör başarıyla oluşturuldu");
                System.out.println("Klasör oluşturuldu.");
            } catch (
                    IOException e) {
                LOGGER.error("Klasör oluşturulamadı." + e.getMessage());
                e.printStackTrace();
            }
            if (dbBckDirName.equals("mysql")) {

                extension = ".sql";
                dosyaIsmi = new DateTime().toString(DATE_FORMAT) + ".sql";
                String backupFilePath = dbBckPath + dosyaIsmi;

                try (Connection connection = DriverManager.getConnection(url, user, password)) {
                    backupMysqlDb(connection, backupFilePath, user, password, url, dbName);
                    LOGGER.info("Yedekleme başarıyla tamamlandı.");
                    book.setDbBackUpName(dosyaIsmi);
                    System.out.println("Veritabanı yedekleme işlemi başarıyla tamamlandı.");
                    isBacked = true;
                } catch (SQLException | IOException e) {
                    LOGGER.error("Yedekleme başarısız oldu." + e.getMessage());
                    e.printStackTrace();
                }

            } else if (dbBckDirName.equals("mssql")) {

                extension = ".bak";
                dosyaIsmi = new DateTime().toString(DATE_FORMAT) + ".bak";
                String backupFilePath = dbBckPath + dosyaIsmi;

                try (Connection connection = DriverManager.getConnection(url, user, password)) {
                    boolean success = BackUpUtility.backupMssqlDb(connection, backupFilePath, user, password, url, dbName);
                    LOGGER.info("Yedekleme başarıyla tamamlandı.");
                    book.setDbBackUpName(dosyaIsmi);
                    System.out.println("Veritabanı yedekleme işlemi başarıyla tamamlandı.");
                    isBacked = true;
                } catch (SQLException | IOException e) {
                    LOGGER.error("Yedekleme başarısız oldu." + e.getMessage());
                    e.printStackTrace();
                }

            } else {

                throw new RuntimeException(Error.CODE_102, Error.SUB_CODE_102039);
            }
            // TODO minIO' ya kaydeden kısım
            if (isBacked) {
                try {
                    String minIOEndPoint = customer.getMinioEndpoint();
                    if (customer.getMinioPort() != null && customer.getMinioPort() != 0) {
                        minIOEndPoint = minIOEndPoint + ":" + customer.getMinioPort();
                    }

                    //TODO minIO bağlantı bilgilerinin çekildiği yer
                    MinioClient minioClient = MinIOUtil.getMinIOConnection(minIOEndPoint, customer.getMinioAccessKey(), customer.getMinioSecretKey());

                    sendFileToMinio(credentials, dosyaIsmi, dbBckPath, minioClient, customer.getMinioBucketName(), archiveFilePath, extension);
                } catch (Exception e) {
                    throw e;
                }
                book.setMinIOUploadDate(minIOUploadDate);
                //book.setMinIOUpload(true);
                book.setMinIOUploadDB(true);
                String resultData = "MinIO yüklemesi başarılı";
                return new Response(true, resultData);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        //TODO minIO ya zip şeklinde dosyayı gönderen kısım

        private void sendFileToMinio (Credentials credentials, String fileName, String filePath, MinioClient
        minioClient, String bucketName, String archiveFilePath, String extension){
        if (Validation.isEmpty(fileName)) {
            return;
        }
        if (fileName.lastIndexOf("zip") < 0) {
            fileName = fileName.replace(extension, ".zip");
            try {
                createZip(filePath + fileName.replace(".zip", extension), filePath + fileName);
            } catch (IOException e) {
                throw new RuntimeException(Error.CODE_195, SUB_CODE_195005);
            }
        }
        byte[] fileBytes = getFileByteArray(filePath + fileName);
        String newPath = "dbBackUp/";
        MinIODTO dto = new MinIODTO(fileBytes);
        boolean result = putObject(minioClient, bucketName, dto, fileName, newPath);
        if (!result) {
            saveLogMinIO(credentials.getUsername(), fileName, UPLOADCLOUDERROR);
            throw new RuntimeException(Error.CODE_195, SUB_CODE_195004 + fileName);
        }
        saveLogMinIO(credentials.getUsername(), fileName, UPLOADCLOUD);
    }
    }
}
