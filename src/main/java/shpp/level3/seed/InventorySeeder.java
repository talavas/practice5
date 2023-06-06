package shpp.level3.seed;

import com.google.firebase.database.*;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.level3.util.FireBaseService;
import shpp.level3.util.RandomGenerator;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class InventorySeeder {
    private final Logger logger = LoggerFactory.getLogger("console");
    private final FireBaseService fireBaseService;

    private final AtomicInteger inventoryCounter = new AtomicInteger(0);
    public InventorySeeder(FireBaseService fireBaseService) {
        this.fireBaseService = fireBaseService;
    }
    public void generateInventory() {

        Map<String, Object> products = getProducts();
        Set<String> storeKeys = getStoreKeys();
        logger.info("Generate inventory {} from {} products for {} stores.",
                storeKeys.size() * products.size(), products.size(), storeKeys.size());
        StopWatch timer = new StopWatch();
        timer.start();
        int counter = 0;
        ExecutorService executor = Executors.newCachedThreadPool();
        List<CompletableFuture<Void>> futures = new ArrayList<>();


        for (String storeKey : storeKeys) {
            for (Map.Entry<String, Object> product : products.entrySet()) {
                Map<String, Object> productData = (Map<String, Object>) product.getValue();
                int quantity = RandomGenerator.getRandom().nextInt(10)+1;
                CompletableFuture<Void> future = CompletableFuture.runAsync(() ->
                        insertInventory(storeKey, product.getKey(), (String) productData.get("type"), quantity)
                        , executor);

                futures.add(future);
                counter++;
                if(counter % 100 == 0){
                    logger.info("Generated {} inventory", counter);
                    CompletableFuture<Void> allFutures = CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));

                    try {
                        allFutures.get();
                        futures.clear();
                    } catch (InterruptedException | ExecutionException e) {
                        logger.error("Error generating inventory: {}", e.getMessage());
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

        try {
            allFutures.get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error generating inventory: {}", e.getMessage());
            Thread.currentThread().interrupt();
        }

        executor.shutdown();
        timer.stop();
        logger.info("Inventory generation completed successfully.");
        logger.info("Generated {} inventory with RPS={}", inventoryCounter, ((double)inventoryCounter.get() / timer.getTime(TimeUnit.MILLISECONDS)) * 1000);
    }
    private Map<String, Object> getProducts() {
        CountDownLatch done = new CountDownLatch(1);
        StopWatch timer = new StopWatch();
        timer.start();
        DatabaseReference dbRef = fireBaseService.getDb().getReference();
        final Map<String, Object>[] products = new Map[]{new HashMap<>()};
        logger.info("Get products collection.");
        dbRef.child("products").addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                if (dataSnapshot.exists()) {

                        products[0] = (Map<String, Object>) dataSnapshot.getValue();
                        logger.info("Receive {} products for {}ms", products[0].size(), timer.getTime(TimeUnit.MILLISECONDS));
                }
                done.countDown();
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {
                logger.error("Canceled insertion", databaseError.toException());
                done.countDown();
            }
        });

        try {
            done.await();
        } catch (InterruptedException e) {
            logger.error("Interrupt insert");
            Thread.currentThread().interrupt();
        }

        return products[0];
    }
    private Set<String> getStoreKeys() {
        CountDownLatch done = new CountDownLatch(1);
        StopWatch timer = new StopWatch();
        timer.start();
        DatabaseReference dbRef = fireBaseService.getDb().getReference();
        final Set<String> keys = new HashSet<>();
        logger.info("Get stores keys");
        dbRef.child("stores").addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                if (dataSnapshot.exists()) {
                        Map<String, Object> stores = (Map<String, Object>) dataSnapshot.getValue();
                        keys.addAll(stores.keySet());
                    logger.info("Receive {} stores keys for {}ms", stores.size(), timer.getTime(TimeUnit.MILLISECONDS));
                }
                done.countDown();
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {
                done.countDown();
            }
        });

        try {
            done.await();
        } catch (InterruptedException e) {
            logger.error("Interrupt insert");
            Thread.currentThread().interrupt();
        }

        logger.debug("Store Keys = {}", keys);
        return keys;
    }


    private void insertInventory(String storeKey, String productTypeKey, String typeUid, Integer quantity) {
        DatabaseReference inventory = fireBaseService.getDb().getReference("inventory").push();
        Map<String, Object> inventoryData = new HashMap<>();
        inventoryData.put("store_uid", storeKey);
        inventoryData.put("product_uid", productTypeKey);
        inventoryData.put("product_type_uid", typeUid);
        inventoryData.put("quantity", quantity);

        CompletableFuture<Void> inventoryFuture = setAsyncValue(inventory, inventoryData);
        inventoryFuture.join();


    }

    private CompletableFuture<Void> setAsyncValue(DatabaseReference dbRef, Object value) {
        logger.debug("set value = {} to ref={}", value, dbRef.getRef());
        CompletableFuture<Void> future = new CompletableFuture<>();

        dbRef.setValue(value, (databaseError, databaseReference) -> {
            if(databaseError == null){
                logger.debug("Value set. {}", value);
                inventoryCounter.incrementAndGet();
                future.complete(null);
            }else{
                future.completeExceptionally(new Throwable("Can't set value"));
            }
        });

        return future;
    }

}
