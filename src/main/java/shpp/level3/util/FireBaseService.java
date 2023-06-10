package shpp.level3.util;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.database.*;
import com.google.firebase.internal.EmulatorCredentials;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class FireBaseService {
    private static final Logger logger = LoggerFactory.getLogger("console");
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
        StopWatch timer = new StopWatch();
        timer.start();
        CompletableFuture<Void> removeTask = new CompletableFuture<>();
        DatabaseReference dbRef = getDb().getReference(collectionName);

        logger.info("Remove collection {}", dbRef);
        dbRef.removeValue((databaseError, databaseReference) -> {
            if(databaseError != null) {
                logger.debug("Can't remove collection {}", collectionName);
            }else{
                logger.info("Remove {} collection for {}ms.", collectionName, timer.getTime(TimeUnit.MILLISECONDS));
            }
                removeTask.complete(null);

        });
        return removeTask;
    }


    public FirebaseDatabase getDb() {
        return db;
    }

    public Set<String> getKeys(String path){
        logger.info("Get keys from collection path={}", path);
        StopWatch timer = new StopWatch();
        CompletableFuture<Set<String>> getKeysAsync = new CompletableFuture<>();
        timer.start();
        DatabaseReference dbRef = getDb().getReference(path);
        dbRef.addValueEventListener(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                if (dataSnapshot.exists()) {
                    Map<String, Object> types = (Map<String, Object>) dataSnapshot.getValue();
                    logger.debug("Get keys snap shot = {}", dataSnapshot);
                    getKeysAsync.complete(types.keySet());
                }
                else{
                    getKeysAsync.complete(null);
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {
                logger.error("Canceled read collection from path = {}.", path);
                getKeysAsync.complete(null);
            }
        });

        Set<String> keys = new HashSet<>();
        try {
            keys = getKeysAsync.get(10, TimeUnit.SECONDS);
            logger.info("Receive {} keys from path={} for {} ms",keys.size(), path, timer.getTime(TimeUnit.MILLISECONDS));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            logger.error("Can't receive products list", e);
            Thread.currentThread().interrupt();
        }
        timer.stop();
        timer.reset();

        logger.debug("Received keys = {}", keys);
        return keys;
    }

}
