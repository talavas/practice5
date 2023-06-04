package shpp.level3.seed;

import com.google.api.core.ApiFuture;
import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.ValueEventListener;
import com.google.firebase.database.snapshot.ChildKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.level3.util.FireBaseService;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public class InventoryUpdater {
    private final Logger logger = LoggerFactory.getLogger(InventoryUpdater.class);
    private final FireBaseService fireBaseService;

    public InventoryUpdater(FireBaseService fireBaseService) {
        this.fireBaseService = fireBaseService;
    }
    public Map<String, Object> getProductTypes() {
        CountDownLatch done = new CountDownLatch(1);
        DatabaseReference dbRef = fireBaseService.getDb().getReference();
        final Map<String, Object>[] types = new Map[]{new HashMap<>()};
        dbRef.child("product-types").addValueEventListener(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                if (dataSnapshot.exists()) {
                    types[0] = (Map<String, Object>) dataSnapshot.getValue();
                    done.countDown();
                } else {
                    done.countDown();
                }
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
        logger.debug("Types = {}", types[0]);
        return types[0];
    }


    private void createInventoryByTypes(Map<String, Object> types){
        CountDownLatch done = new CountDownLatch(1);
        DatabaseReference inventoryByType = fireBaseService.getDb().getReference("inventory-by-type");

    }
    public void updateInventoryByType(Map<String, Object> inventory) {
        CountDownLatch done = new CountDownLatch(1);
        logger.info("Received inventory = {}", inventory);
        //String quantity = (String) inventory.get("quantity");
        String productUID = (String) inventory.get("product_uid");
        String storeUID = (String) inventory.get("store_uid");
        logger.info("product-uid={}, store-uid={}, quantity=", productUID, storeUID);
        DatabaseReference typeRef = fireBaseService.getDb().getReference("products").child(productUID).child("type");
        typeRef.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                String type = (String) dataSnapshot.getValue();
                logger.info("TypeRef = {}", type);
                DatabaseReference inventoryByType = fireBaseService.getDb().getReference("inventory-by-types");
                inventoryByType.child(type).addListenerForSingleValueEvent(new ValueEventListener() {
                    @Override
                    public void onDataChange(DataSnapshot snapshot) {
                        logger.info("Data change = {}", snapshot);
                        int newQuantity = 0;
                        if (snapshot.exists()) {
                            Map<String, Object> inventoryByTypeData = snapshot.getValue(Map.class);
                            int existingQuantity = (int) inventoryByTypeData.get("quantity");
                            newQuantity += existingQuantity;
                        }

                        Map<String, Object> inventoryByTypeData = new HashMap<>();
                        inventoryByTypeData.put("store_uid", storeUID);
                        inventoryByTypeData.put("quantity", newQuantity);

                        ApiFuture<Void> future = inventoryByType.child(type).setValueAsync(inventoryByTypeData);
                        try {
                            future.get(10, TimeUnit.SECONDS);
                        } catch (InterruptedException | ExecutionException | TimeoutException e) {
                            throw new RuntimeException(e);
                        }
                        done.countDown();
                    }

                    @Override
                    public void onCancelled(DatabaseError error) {
                        logger.error("Failed to read inventory by type for type: {}", typeRef);
                        done.countDown();
                    }
                });

                try {
                    done.await();
                } catch (InterruptedException e) {
                    logger.error("Interrupt insert");
                    e.printStackTrace();
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });

    }

    private CompletableFuture<String> getProductType(String productUID) {
        CompletableFuture<String> future = new CompletableFuture<>();

        DatabaseReference dbRef = fireBaseService.getDb().getReference("products").child(productUID).child("type");
        dbRef.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                logger.info("Receive type = {}", dataSnapshot);
                if (dataSnapshot.exists()) {
                    String type = dataSnapshot.getValue(String.class);
                    future.complete(type);
                } else {
                    future.complete(null);
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {
                future.completeExceptionally(databaseError.toException());
            }
        });

        return future;
    }
}
