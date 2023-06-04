package shpp.level3.seed;

import com.google.api.core.ApiFuture;
import com.google.firebase.database.*;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.level3.model.Product;
import shpp.level3.util.FireBaseService;
import shpp.level3.util.RandomGenerator;

import java.security.KeyStore;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class InventorySeeder {
    private final Logger logger = LoggerFactory.getLogger(InventorySeeder.class);
    private final FireBaseService fireBaseService;

    public InventorySeeder(FireBaseService fireBaseService) {
        this.fireBaseService = fireBaseService;
    }
    public void generateInventory() {

       // updateInventoryByType();
        Map<String, Object> products = getProducts();
        Set<String> storeKeys = getStoreKeys();
        logger.info("Generate inventory. Stores {}", storeKeys.size());
        StopWatch timer = new StopWatch();
        timer.start();
        int counter = 0;

        for (String storeKey : storeKeys) {
            for (Map.Entry<String, Object> product : products.entrySet()) {
                Map<String, Object> productData = (Map<String, Object>) product.getValue();
                int quantity = 1;
                updateInventory(storeKey, product.getKey(), (String) productData.get("type"), quantity);
                counter++;
            }
        }

        logger.info("Inventory generation completed successfully.");
        logger.info("Generated {} inventory with RPS={}", counter, ((double)counter / timer.getTime(TimeUnit.MILLISECONDS)) * 1000);
    }
    private Map<String, Object> getProducts() {
        CountDownLatch done = new CountDownLatch(1);
        DatabaseReference dbRef = fireBaseService.getDb().getReference();
        final Map<String, Object>[] products = new Map[]{new HashMap<>()};
        logger.info("Get products");
        dbRef.child("products").addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                if (dataSnapshot.exists()) {

                        products[0] = (Map<String, Object>) dataSnapshot.getValue();
                        logger.info("get {} products", products[0].size());
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
            e.printStackTrace();
        }

        logger.info("Products = {}", products[0]);
        return products[0];
    }
    private Set<String> getStoreKeys() {
        CountDownLatch done = new CountDownLatch(1);
        DatabaseReference dbRef = fireBaseService.getDb().getReference();
        final Set<String> keys = new HashSet<>();
        logger.debug("Get stores keys");
        dbRef.child("stores").addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                if (dataSnapshot.exists()) {
                        Map<String, Object> stores = (Map<String, Object>) dataSnapshot.getValue();
                        keys.addAll(stores.keySet());
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
            e.printStackTrace();
        }

        logger.debug("Store Keys = {}", keys);
        return keys;
    }

    private void updateInventory(String storeKey, String productTypeKey, String typeUid, Integer quantity) {
        DatabaseReference inventory = fireBaseService.getDb().getReference("inventory").push();
        Map<String, Object> inventoryData = new HashMap<>();
        inventoryData.put("store_uid", storeKey);
        inventoryData.put("product_uid", productTypeKey);
        inventoryData.put("product_type_uid", typeUid);
        inventoryData.put("quantity", quantity);
        updateInventoryByType(inventoryData);
        ApiFuture<Void> future = inventory.setValueAsync(inventoryData);
        try {
            future.get(10, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
    private void updateInventoryByType(Map<String, Object> inventoryData) {
        String productTypeUid = (String) inventoryData.get("product_type_uid");
        String storeUid = (String) inventoryData.get("store_uid");
        Integer quantity = (Integer) inventoryData.get("quantity");
        DatabaseReference inventory = fireBaseService.getDb().getReference("inventory-by-type")
                .child(productTypeUid)
                .child("stores")
                .child(storeUid);
        logger.debug("type_uid={} store_id = {} quantity = {}", productTypeUid, storeUid, quantity);
        inventory.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                Map<String, Object> inventoryByType = new HashMap<>();

                if(dataSnapshot.exists()){
                    logger.info("quantity1 = {}, quantity={}", dataSnapshot.child("quantity").getValue(), quantity);
                    Integer quantity1 = dataSnapshot.child("quantity").getValue(Integer.class);
                    inventoryByType.put("quantity", quantity1 + quantity);
                    logger.info("Found inventory quantity = {}", quantity1);
                }else{
                    inventoryByType.put("quantity", quantity);
                }

                inventory.setValueAsync(inventoryByType);
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });

    }


}
