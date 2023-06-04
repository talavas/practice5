package shpp.level3.seed;

import com.google.api.core.ApiFuture;
import com.google.firebase.database.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.level3.util.FireBaseService;

import java.util.HashMap;
import java.util.Map;

public class InventoryByTypeListener implements ChildEventListener {
    private final Logger logger = LoggerFactory.getLogger("console");
    private final FireBaseService fireBaseService;
    InventoryUpdater inventoryUpdater;

    public InventoryByTypeListener(FireBaseService fireBaseService) {
        this.fireBaseService = fireBaseService;
        this.inventoryUpdater = new InventoryUpdater(fireBaseService);
    }

    @Override
    public void onChildAdded(DataSnapshot dataSnapshot, String previousChildKey) {
        Map<String, Object> inventory = (Map<String, Object>) dataSnapshot.getValue();
        inventoryUpdater.updateInventoryByType(inventory);
    }

    @Override
    public void onChildChanged(DataSnapshot dataSnapshot, String previousChildKey) {
        // Not applicable in this case
    }

    @Override
    public void onChildRemoved(DataSnapshot dataSnapshot) {
        // Not applicable in this case
    }

    @Override
    public void onChildMoved(DataSnapshot dataSnapshot, String previousChildKey) {
        // Not applicable in this case
    }

    @Override
    public void onCancelled(DatabaseError databaseError) {
        logger.error("Failed to update inventory by type");
    }

}
