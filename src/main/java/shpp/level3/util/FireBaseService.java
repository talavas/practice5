package shpp.level3.util;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.internal.EmulatorCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class FireBaseService {
    private final Logger logger = LoggerFactory.getLogger("console");
    Config config;
    FirebaseDatabase db;

    public FireBaseService(Config config) {
        this.config = config;

        try {
            init(config);
        } catch (IOException e) {
            logger.error("Can't connect to db: ", e);
        }

    }

    public void clearDatabase() {
        List<CompletableFuture<Void>> futureTasks = new ArrayList<>();
        for (DBReferences collection : DBReferences.values()) {
            CompletableFuture<Void> removeTask = removeCollection(collection.getName());
            futureTasks.add(removeTask);
        }
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(futureTasks.toArray(CompletableFuture[]::new));

        try {
            allFutures.get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error removing collections: {}", e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    private void init(Config config) throws IOException {
        FirebaseOptions firebaseOptions;
        logger.debug("Create FirebaseOptions.");
        if (config.getProperty("emulate").equals("true")) {
            firebaseOptions = getEmulatorFirebase();
        } else {
            firebaseOptions = getProductionFirebase();
        }

        if (firebaseOptions != null) {
            logger.info("Init Firebase App, {}", firebaseOptions.getDatabaseUrl());
            FirebaseApp app = FirebaseApp.initializeApp(firebaseOptions);
            this.db = FirebaseDatabase.getInstance(app);
        } else {
            logger.debug("Firebase Option is null");
        }
    }


    private FirebaseOptions getProductionFirebase() throws IOException {
        logger.info("Get connection details and create Firebase Options.");

        InputStream serviceAccount = getClass().getClassLoader().getResourceAsStream(config.getProperty("auth"));

        if(serviceAccount != null){
            return FirebaseOptions.builder()
                    .setCredentials(GoogleCredentials.fromStream(serviceAccount))
                    .setDatabaseUrl(config.getProperty("database.url"))
                    .build();

        } else {
            logger.error("Can't open file with credentials.");
        }
        return null;
    }

    private FirebaseOptions getEmulatorFirebase() {
        logger.info("Get options for emulator");
        return FirebaseOptions.builder()
                .setCredentials(new EmulatorCredentials())
                .setProjectId("demo-practice5")
                .setDatabaseUrl(config.getProperty("database.url"))
                .build();
    }

    private CompletableFuture<Void> removeCollection(String collectionName){
        CompletableFuture<Void> removeTask = new CompletableFuture<>();
        DatabaseReference dbRef = getDb().getReference(collectionName);
        logger.debug("Remove collection {}", dbRef);
        dbRef.removeValue((databaseError, databaseReference) -> {
            if(databaseError != null) {
                logger.debug("Can't remove collection {}", collectionName);
            }else{
                logger.debug("Remove {} collection.", collectionName);
            }
                removeTask.complete(null);

        });
        return removeTask;
    }


    public FirebaseDatabase getDb() {
        return db;
    }

}
