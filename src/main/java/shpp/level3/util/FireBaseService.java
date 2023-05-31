package shpp.level3.util;

import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.*;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.internal.EmulatorCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class FireBaseService {
    private final Logger logger = LoggerFactory.getLogger(FireBaseService.class);

    public Config getConfig() {
        return config;
    }

    Config config;

    FirebaseDatabase db;

    public Firestore getFirestore() {
        return firestore;
    }

    Firestore firestore;

    public int getBatchSize() {
        return batchSize;
    }

    private int batchSize;


    public FireBaseService(Config config) throws IOException {
        this.config = config;
        this.batchSize = Integer.parseInt(config.getProperty("batch.size"));
        File file = new File(
               getClass().getClassLoader().getResource(config.getProperty("auth.google.json")).getFile()
        );

        FileInputStream inputStream = new FileInputStream(file);
        logger.debug("Create FirebaseOptions.");
        FirestoreOptions firestoreOptions = FirestoreOptions.newBuilder()
                .setCredentials(new EmulatorCredentials())
                .setEmulatorHost("127.0.0.1:8081")
                .setProjectId("practice5")
                .build();
        FirebaseOptions options = FirebaseOptions.builder()
                .setCredentials(new EmulatorCredentials())
                .setProjectId("practice5")
                .setDatabaseUrl(config.getProperty("database.url"))
                .build();

        logger.debug("Init Firebase App");
        FirebaseApp app = FirebaseApp.initializeApp(options);

        db = FirebaseDatabase.getInstance();
        inputStream = new FileInputStream(file);
//        FirestoreOptions firestoreOptions = FirestoreOptions.newBuilder()
//                .setProjectId("practice5")
//                .setCredentials(GoogleCredentials.fromStream(inputStream))
//                .build();
//        FirestoreOptions firestoreOptions = FirestoreOptions.newBuilder()
//                .setCredentials(GoogleCredentials.fromStream(inputStream))
//                .setProjectId("practice5") // Замініть на свій ID проекту
//                .setHost("127.0.0.1:8081")
//                .build();
        firestore = firestoreOptions.getService();
    }

    public CollectionReference getCollection(String collection){
        return firestore.collection(collection);
    }

    public FirebaseDatabase getDb() {
        return db;
    }

    public void closeFirestore() throws Exception {
        this.firestore.close();
    }


    public void deleteCollection(String collectionName) {
        logger.info("Deleting collection {}", collectionName);
        CollectionReference collection = getCollection(collectionName);

        try {
            // Get all documents in the collection
            ApiFuture<QuerySnapshot> future = collection.get();
            QuerySnapshot snapshot = future.get();
            List<QueryDocumentSnapshot> documents = snapshot.getDocuments();

            // Create a batch to perform the deletion
            WriteBatch batch = collection.getFirestore().batch();
            for (QueryDocumentSnapshot document : documents) {
                batch.delete(document.getReference());
            }

            // Commit the batch to delete all documents
            batch.commit();
            logger.debug("Collection {} deleted.", collectionName);
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error deleting collection.", e);
        }
    }

    public long getDocumentCount(String collectionName) {
        CollectionReference collection = getCollection(collectionName);

        try {
            ApiFuture<QuerySnapshot> future = collection.get();
            QuerySnapshot snapshot = future.get();

            // Отримуємо кількість документів у колекції
            long count = snapshot.size();
            return count;
        } catch (InterruptedException | ExecutionException e) {
            // Обробка винятку при отриманні результату
            logger.error("Error getting document count.", e);
        }

        return -1; // Повертаємо -1 у разі помилки
    }
}
