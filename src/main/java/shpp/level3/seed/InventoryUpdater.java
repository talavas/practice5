package shpp.level3.seed;

import com.google.firebase.database.*;

import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.level3.util.DBReferences;
import shpp.level3.util.FireBaseService;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

public class InventoryUpdater {
    private final Logger logger = LoggerFactory.getLogger("console");
    private final DatabaseReference inventoryByTypeRef;
    private final FireBaseService fireBaseService;
    Set<String> productTypesKeys;

    public InventoryUpdater(FireBaseService fireBaseService) {
        this.fireBaseService = fireBaseService;
        this.inventoryByTypeRef = fireBaseService.getDb().getReference(DBReferences.INVENTORY_BY_TYPE.getName());
        this.productTypesKeys = fireBaseService.getKeys(DBReferences.PRODUCT_TYPES.getName());

    }

    public void update(){
        StopWatch timer = new StopWatch();
        timer.start();
        logger.info("Creating collection inventory-by-type.");
        for(String typeUid : productTypesKeys){
            updateInventoryByType(typeUid);
        }
        logger.info("Collection inventory-by-type created, time exec = {}ms", timer.getTime(TimeUnit.MILLISECONDS));
    }

    private void updateInventoryByType(String typeUid) {
        CompletableFuture<Void> updateInventory = new CompletableFuture<>();
        DatabaseReference inventoryRef = fireBaseService.getDb().getReference("inventory").child(typeUid);
        logger.debug("Inventory Ref = {}", inventoryRef.getRef());

        inventoryRef.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                logger.debug("Inventory snapShot = {}", dataSnapshot);
                Map<String, Integer> inventoryByStore = new HashMap<>();

                for (DataSnapshot snapshot : dataSnapshot.getChildren()) {
                    logger.debug("Children snapshot = {}", snapshot);

                    String storeKey = snapshot.getKey();
                    for(DataSnapshot snapshot1 : snapshot.getChildren()) {
                        Map<String, Object> productInventory = (Map<String, Object>) snapshot1.getValue();
                        Integer quantity = Integer.parseInt(productInventory.get("quantity").toString());
                        logger.debug("StoreKey = {}, quantity = {}", storeKey, quantity);
                        if (inventoryByStore.containsKey(storeKey)) {
                            int existingQuantity = inventoryByStore.get(storeKey);
                            inventoryByStore.put(storeKey, existingQuantity + quantity);
                        } else {
                            inventoryByStore.put(storeKey, quantity);
                        }
                    }
                }
                DatabaseReference inventoryByType = inventoryByTypeRef
                        .child(typeUid)
                        .child("stores");

                inventoryByType.setValueAsync(inventoryByStore);
                updateInventory.complete(null);
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {
               logger.debug("Canceled query {}", databaseError.getMessage());
               updateInventory.complete(null);

            }
        });
        try {
            updateInventory.get(10, TimeUnit.SECONDS);
            logger.debug("Inventory for typeUid={} updated", typeUid);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            logger.error("Can't update inventory-by-type.", e);
            Thread.currentThread().interrupt();
        }
    }
}
