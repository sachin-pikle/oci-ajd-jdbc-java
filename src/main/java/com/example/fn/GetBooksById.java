package com.example.fn;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.oracle.bmc.Region;
import com.oracle.bmc.auth.ResourcePrincipalAuthenticationDetailsProvider;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import com.oracle.bmc.objectstorage.requests.ListObjectsRequest;
import com.oracle.bmc.objectstorage.responses.GetObjectResponse;
import com.oracle.bmc.objectstorage.responses.ListObjectsResponse;
import oracle.soda.*;
import oracle.soda.rdbms.OracleRDBMSClient;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

public class GetBooksById {

    private PoolDataSource poolDataSource;

    private final File walletDir = new File("/tmp", "wallet");
    private final String namespace = System.getenv().get("NAMESPACE");
    private final String bucketName = System.getenv().get("BUCKET_NAME");
    private final String dbUser = System.getenv().get("DB_USER");
    private final String dbPassword = System.getenv().get("DB_PASSWORD");
    private final String tnsName = System.getenv().get("TNS_NAME");
    private final String dbUrl = "jdbc:oracle:thin:@" + tnsName + "?TNS_ADMIN=/tmp/wallet";

    private final String collectionName = System.getenv().get("COLLECTION_NAME");

    final static String CONN_FACTORY_CLASS_NAME="oracle.jdbc.pool.OracleDataSource";

    public GetBooksById() {
        System.out.println("In constructor GetBooksById(). Setting up pool data source");
        poolDataSource = PoolDataSourceFactory.getPoolDataSource();
        try {
            poolDataSource.setConnectionFactoryClassName(CONN_FACTORY_CLASS_NAME);
            poolDataSource.setURL(dbUrl);
            poolDataSource.setUser(dbUser);
            poolDataSource.setPassword(dbPassword);
            poolDataSource.setConnectionPoolName("UCP_POOL");
            poolDataSource.setInitialPoolSize(1);
            poolDataSource.setMinPoolSize(1);
            poolDataSource.setMaxPoolSize(1);
            System.out.println("Pool data source setup complete.");
        }
        catch (SQLException e) {
            System.out.println("Pool data source error!");
            e.printStackTrace();
        }
    }

    public String handleRequest(String input) throws SQLException, JsonProcessingException, OracleException {
        System.out.println("Entering GetBooksById::handleRequest().");
        System.setProperty("oracle.jdbc.fanEnabled", "false");
        String name = (input == null || input.isEmpty()) ? "world"  : input;

        if( needWalletDownload() ) {
            System.out.println("Start wallet download...");
            downloadWallet();
            System.out.println("End wallet download!");
        } else {
            System.out.println("Wallet exists. Not going to download.");
        }

        System.out.println("DB_URL = " + dbUrl);
        System.out.println("DB_USER = " + dbUser);
        System.out.println("COLLECTION_NAME = " + collectionName);

        System.out.println("*****");
        getAll();

/*
    076DA47A4377430C9239D1098400B259		2021-01-04T13:53:10.365240000Z
	1A9B0CBFB1D8411E88E140E9DAD19D76		2021-01-04T13:53:28.793604000Z
	DA8803A9BFB14070964D4DB49478C6FA		2021-01-04T13:53:28.801371000Z
	4DCEFCD41B8345DC8B1035598252BEEC		2021-01-04T13:53:28.808104000Z
	ED411A2E5BA34705ADA4835C9C37FF39
*/
        return getById("076DA47A4377430C9239D1098400B259");
    }

    public String getById(String id) {
        System.out.println("Entering GetBooksById::getById().");
        try (Connection conn = poolDataSource.getConnection()) {
            OracleRDBMSClient cl = new OracleRDBMSClient();
            // Get a database.
            OracleDatabase db = cl.getDatabase(conn);
            OracleCollection col = db.openCollection(collectionName);
            OracleDocument doc = col.findOne(id);
            return doc.getContentAsString();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public void getAll() {
        System.out.println("Entering GetBooksById::getAll().");
        try (Connection conn = poolDataSource.getConnection()) {
            OracleRDBMSClient cl = new OracleRDBMSClient();
            // Get a database.
            OracleDatabase db = cl.getDatabase(conn);
            OracleCollection col = db.openCollection(collectionName);
            OracleCursor c = col.find().getCursor();

            while (c.hasNext()) {
                OracleDocument resultDoc = c.next();
                System.out.println("Document content: " +
                        resultDoc.getContentAsString());
            }
            // IMPORTANT: You must close the cursor to release resources!
            c.close();
            System.out.println("Exiting GetBooksById::getAll().");
//            return doc.getContentAsString();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private Boolean needWalletDownload() {
        if( walletDir.exists() ) {
            System.out.println("Wallet exists, don't download it again...");
            return false;
        }
        else {
            System.out.println("Didn't find a wallet, let's download one...");
            walletDir.mkdirs();
            return true;
        }
    }

    private void downloadWallet() {
        // Use Resource Principal
        final ResourcePrincipalAuthenticationDetailsProvider provider =
                ResourcePrincipalAuthenticationDetailsProvider.builder().build();

        ObjectStorage client = new ObjectStorageClient(provider);
        client.setRegion(Region.US_ASHBURN_1);

        System.out.println("Retrieving a list of all objects in /" + namespace + "/" + bucketName + "...");
        // List all objects in wallet bucket
        ListObjectsRequest listObjectsRequest = ListObjectsRequest.builder()
                .namespaceName(namespace)
                .bucketName(bucketName)
                .build();
        ListObjectsResponse listObjectsResponse = client.listObjects(listObjectsRequest);
        System.out.println("List retrieved. Starting download of each object...");

        // Iterate over each wallet file, downloading it to the Function's Docker container
        listObjectsResponse.getListObjects().getObjects().stream().forEach(objectSummary -> {
            System.out.println("Downloading wallet file: [" + objectSummary.getName() + "]");

            GetObjectRequest objectRequest = GetObjectRequest.builder()
                    .namespaceName(namespace)
                    .bucketName(bucketName)
                    .objectName(objectSummary.getName())
                    .build();
            GetObjectResponse objectResponse = client.getObject(objectRequest);

            try {
                File f = new File(walletDir + "/" + objectSummary.getName());
                FileUtils.copyToFile( objectResponse.getInputStream(), f );
                System.out.println("Stored wallet file: " + f.getAbsolutePath());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

}