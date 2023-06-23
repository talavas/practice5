package shpp.level3.seed;

import com.google.firebase.database.*;
import org.junit.jupiter.api.Assertions;
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

class InventorySeederTest {
    @Mock
    private FireBaseService fireBaseService;

    @Mock
    private DatabaseReference databaseReference;
    @Mock
    private FirebaseDatabase firebaseDatabase;

    @Mock
    private Query query;

    @Mock
    private DataSnapshot dataSnapshot;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void generateInventory_shouldGenerateInventorySuccessfully() {
        Set<String> storesKeys = new HashSet<>(Arrays.asList("store1", "store2"));
        Set<String> productTypesKeys = new HashSet<>(Arrays.asList("type1"));
        Map<String, Object> products = new HashMap<>();
        products.put("product1", createProductMap("type1"));
        products.put("product2", createProductMap("type1"));

        when(fireBaseService.getKeys(DBReferences.STORES.getName())).thenReturn(storesKeys);
        when(fireBaseService.getKeys(DBReferences.PRODUCT_TYPES.getName())).thenReturn(productTypesKeys);
        when(fireBaseService.getDb()).thenReturn(firebaseDatabase);
        when(firebaseDatabase.getReference(anyString())).thenReturn(databaseReference);
        when(databaseReference.child(anyString())).thenReturn(databaseReference);
        when(databaseReference.orderByChild(anyString())).thenReturn(databaseReference);
        when(databaseReference.equalTo(anyString())).thenReturn(query);
        doAnswer(invocation -> {
            ValueEventListener listener = invocation.getArgument(0);
            listener.onDataChange(dataSnapshot);
            return null;
        }).when(query).addListenerForSingleValueEvent(any());
        when(dataSnapshot.exists()).thenReturn(true);
        when(dataSnapshot.getValue()).thenReturn(products);
        doAnswer(invocation -> {
            Transaction.Handler handler = invocation.getArgument(0);
            handler.doTransaction(mock(MutableData.class));
            handler.onComplete(null, true, dataSnapshot);
            return null;
        }).when(databaseReference).runTransaction(any());

        InventorySeeder inventorySeeder = new InventorySeeder(fireBaseService);

        Assertions.assertEquals(4, inventorySeeder.generateInventory());
        verify(databaseReference, times(4)).runTransaction(any());
        verify(dataSnapshot, times(1)).getValue();

    }

    private Map<String, Object> createProductMap(String typeUid) {
        Map<String, Object> product = new HashMap<>();
        product.put("typeUid", typeUid);
        return product;
    }
}