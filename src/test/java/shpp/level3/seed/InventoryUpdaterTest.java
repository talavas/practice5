package shpp.level3.seed;

import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.database.ValueEventListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import shpp.level3.util.DBReferences;
import shpp.level3.util.FireBaseService;

import java.util.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class InventoryUpdaterTest {
    @Mock
    private FireBaseService fireBaseService;

    @Mock
    private DatabaseReference inventoryByTypeRef;

    @Mock
    private DatabaseReference inventoryRef;
    @Mock
    private FirebaseDatabase firebaseDatabase;

    @Mock
    private DataSnapshot dataSnapshot;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }
    @Test
    void update_shouldUpdateInventoryByTypeSuccessfully() {
        Set<String> productTypesKeys = new HashSet<>(Arrays.asList("typeUid1", "typeUid2"));

        when(fireBaseService.getDb()).thenReturn(firebaseDatabase);
        when(firebaseDatabase.getReference(DBReferences.INVENTORY_BY_TYPE.getName())).thenReturn(inventoryByTypeRef);
        when(fireBaseService.getKeys(DBReferences.PRODUCT_TYPES.getName())).thenReturn(productTypesKeys);
        when(firebaseDatabase.getReference(DBReferences.INVENTORY.getName())).thenReturn(inventoryRef);
        when(inventoryRef.child(anyString())).thenReturn(inventoryRef);

        doAnswer(invocation -> {
            ValueEventListener listener = invocation.getArgument(0);
            listener.onDataChange(dataSnapshot);
            return null;
        }).when(inventoryRef).addListenerForSingleValueEvent(any());

        when(dataSnapshot.getValue()).thenReturn(createStoreDataSnapShot());

        when(inventoryByTypeRef.child(anyString())).thenReturn(inventoryRef);
        when(inventoryByTypeRef.child(anyString()).child(anyString())).thenReturn(inventoryByTypeRef);
        when(inventoryByTypeRef.setValueAsync(any())).thenReturn(null);

        InventoryUpdater inventoryUpdater = new InventoryUpdater(fireBaseService);
        inventoryUpdater.updateInventoryByType("typeUid1", inventoryRef);
        inventoryUpdater.updateInventoryByType("typeUid2", inventoryRef);

        verify(inventoryRef, times(2)).addListenerForSingleValueEvent(any(ValueEventListener.class));
        verify(dataSnapshot, times(2)).getChildren();
        verify(inventoryByTypeRef, times(2)).setValueAsync(any());
    }

    private Map<String, Object> createStoreDataSnapShot(){
        Map<String, Object> stores = new HashMap<>();
        Map<String, Object> store = new HashMap<>();
        Map<String, Object> inventory1 = new HashMap<>();
        Map<String, Object> inventory2 = new HashMap<>();
        Map<String, Object> quantity1 = new HashMap<>();
        Map<String, Object> quantity2 = new HashMap<>();
        quantity1.put("quantity", 1);
        quantity2.put("quantity", 2);
        inventory1.put("productUid1", quantity1);
        inventory2.put("productUid2", quantity2);
        store.put("storeUid1", inventory1);
        store.put("storeUid2", inventory2);
        stores.put("typeUid1", store);
        stores.put("typeUid2", store);
        return stores;
    }
}