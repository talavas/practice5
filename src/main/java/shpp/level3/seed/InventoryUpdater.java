package shpp.level3.seed;

import com.google.firebase.database.*;

import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.level3.util.FireBaseService;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

public class InventoryUpdater {
    private final Logger logger = LoggerFactory.getLogger("console");
    private final DatabaseReference inventoryByTypeRef;
    private final FireBaseService fireBaseService;
    Set<String> types;

    public InventoryUpdater(FireBaseService fireBaseService) {
        this.fireBaseService = fireBaseService;
        this.inventoryByTypeRef = fireBaseService.getDb().getReference("inventory-by-type");
        this.types = new ProductSeeder(fireBaseService).getProductTypeKeys();

    }

    public void update(){
        StopWatch timer = new StopWatch();
        timer.start();
        logger.info("Creating collection inventory-by-type.");
        for(String type : types){
            updateInventoryByType(type);
        }
        logger.info("Collection inventory-by-type created, time exec = {}ms", timer.getTime(TimeUnit.MILLISECONDS));
    }

    public void updateInventoryByType(String typeUid) {
        CountDownLatch done = new CountDownLatch(1);
        DatabaseReference inventoryRef = fireBaseService.getDb().getReference("inventory");
        logger.debug("Inventory Ref = {}", inventoryRef.getRef());
        Query query = inventoryRef.orderByChild("product_type_uid").equalTo(typeUid);

        query.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                logger.debug("Inventory snapShot = {}", dataSnapshot);
                Map<String, Integer> inventoryByStore = new HashMap<>();

                for (DataSnapshot snapshot : dataSnapshot.getChildren()) {
                    String storeKey = snapshot.child("store_uid").getValue(String.class);
                    Integer quantity = snapshot.child("quantity").getValue(Integer.class);

                    if (inventoryByStore.containsKey(storeKey)) {
                        int existingQuantity = inventoryByStore.get(storeKey);
                        inventoryByStore.put(storeKey, existingQuantity + quantity);
                    } else {
                        inventoryByStore.put(storeKey, quantity);
                    }
                    logger.debug("Push inventory storeKey={}", storeKey);
                }

                DatabaseReference inventoryByType = inventoryByTypeRef
                        .child(typeUid)
                        .child("stores");

                inventoryByType.setValueAsync(inventoryByStore);
                done.countDown();
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {
               logger.debug("Canceled query {}", databaseError.getMessage());
               done.countDown();
            }
        });
        try {
            done.await();
        } catch (InterruptedException e) {
            logger.error("Interrupt insert");
           Thread.currentThread().interrupt();
        }
    }
}
