package shpp.level3.seed;

import com.google.firebase.database.*;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.level3.util.DBReferences;
import shpp.level3.util.FireBaseService;
import shpp.level3.util.RandomGenerator;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class InventorySeeder {
    private final Logger logger = LoggerFactory.getLogger("console");
    private final FireBaseService fireBaseService;
    private final StopWatch timer = new StopWatch();

    private final AtomicInteger inventoryCounter = new AtomicInteger(0);
    public InventorySeeder(FireBaseService fireBaseService) {
        this.fireBaseService = fireBaseService;
    }
    public void generateInventory() {
        Set<String> storesKeys = fireBaseService.getKeys(DBReferences.STORES.getName());
        Set<String> productTypesKeys = fireBaseService.getKeys(DBReferences.PRODUCT_TYPES.getName());
        timer.reset();
        timer.start();

        if(!storesKeys.isEmpty() && !productTypesKeys.isEmpty())
            for (String productTypeUid : productTypesKeys){
                Map<String, Object> products = getProducts(productTypeUid);

                if(products != null && !products.isEmpty()){
                    seedInventoryToStores(storesKeys,products);
                    logger.debug("Generate inventory {} from {} products for {} stores.",
                       storesKeys.size() * products.size(), products.size(), storesKeys.size());
                }

            }

        logger.info("Inventory generation completed successfully.");
        logger.info("Generated {} inventory with RPS={}", inventoryCounter, ((double)inventoryCounter.get() / timer.getTime(TimeUnit.MILLISECONDS)) * 1000);
    }

    private void seedInventoryToStores(Set<String> storesKeys, Map<String, Object> products) {
        int counter = 0;
        List<CompletableFuture<Void>> listOfInsertTask = new ArrayList<>();
        for (String storeKey : storesKeys) {
            for (Map.Entry<String, Object> product : products.entrySet()) {
                Map<String, Object> productData = (Map<String, Object>) product.getValue();
                int quantity = RandomGenerator.getRandom().nextInt(10)+1;
                CompletableFuture<Void> insertAsync = CompletableFuture.runAsync(() ->
                        insertInventory(storeKey, product.getKey(), (String) productData.get("typeUid"), quantity)
                );

                listOfInsertTask.add(insertAsync);
                counter++;
                if(counter % 100 == 0){
                    logger.info("Generated {} inventory", counter);
                    CompletableFuture<Void> allInsertTasks = CompletableFuture.allOf(listOfInsertTask.toArray(CompletableFuture[]::new));

                    try {
                        allInsertTasks.get();
                        listOfInsertTask.clear();
                    } catch (InterruptedException | ExecutionException e) {
                        logger.error("Error generating inventory: {}", e.getMessage());
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
        CompletableFuture<Void> allInsertTask = CompletableFuture.allOf(listOfInsertTask.toArray(new CompletableFuture[0]));

        try {
            allInsertTask.get();
            logger.info("Generated {} inventory", counter);
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error generating inventory: {}", e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    private Map<String, Object> getProducts(String productTypeUid) {
        CompletableFuture<Map<String, Object>> getProductsAsync = new CompletableFuture<>();
        DatabaseReference dbRef = fireBaseService.getDb().getReference(DBReferences.PRODUCTS.getName());
        logger.debug("Get products for product_type_uid = {}.", productTypeUid);

        Query query = dbRef.orderByChild("typeUid").equalTo(productTypeUid);
        query.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                if (dataSnapshot.exists()) {
                    getProductsAsync.complete((Map<String, Object>) dataSnapshot.getValue());
                }else{
                    getProductsAsync.complete(null);
                }
            }
            @Override
            public void onCancelled(DatabaseError databaseError) {
                logger.error("Canceled insertion", databaseError.toException());
                getProductsAsync.complete(null);
            }
        });

        Map<String,Object> products = null;
        try {
            products = getProductsAsync.get(10, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            logger.error("Can't receive products list", e);
            Thread.currentThread().interrupt();
        }

        return products;
    }

    private void insertInventory(String storeUid, String productUid, String typeUid, Integer quantity) {
        DatabaseReference inventory = fireBaseService.getDb()
                .getReference("inventory")
                .child(typeUid)
                .child(storeUid)
                .child(productUid)
                .child("quantity");
        CompletableFuture<Void> insertAsync = new CompletableFuture<>();
        inventory.runTransaction(new Transaction.Handler() {
            @Override
            public Transaction.Result doTransaction(MutableData mutableData) {
                Integer savedQuantity = mutableData.getValue(Integer.class);
                if(savedQuantity == null){
                    mutableData.setValue(quantity);
                }else{
                    mutableData.setValue(quantity + savedQuantity);
                }
                inventoryCounter.incrementAndGet();
                insertAsync.complete(null);
                return Transaction.success(mutableData);
            }

            @Override
            public void onComplete(DatabaseError databaseError, boolean b, DataSnapshot dataSnapshot) {
                if(databaseError == null){
                    logger.debug("Insert {} to inventory success", dataSnapshot);
                }else{
                    logger.error("Can't insert inventory {}", databaseError.getMessage());
                }
                insertAsync.complete(null);
            }
        });

        insertAsync.join();
    }

}
