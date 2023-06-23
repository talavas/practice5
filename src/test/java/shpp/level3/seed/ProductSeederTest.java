package shpp.level3.seed;

import com.google.api.core.ApiFuture;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import shpp.level3.model.Product;
import shpp.level3.util.DBReferences;
import shpp.level3.util.FireBaseService;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import static org.mockito.Mockito.*;

class ProductSeederTest {
    @Mock
    private FireBaseService fireBaseService;

    @Mock
    private FirebaseDatabase firebaseDatabase;

    @Mock
    private DatabaseReference databaseReference;

    @Mock
    private ApiFuture<Void> apiFuture;



    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException, TimeoutException {
        MockitoAnnotations.openMocks(this);
        when(fireBaseService.getDb()).thenReturn(firebaseDatabase);
        when(apiFuture.get(anyLong(), any(TimeUnit.class))).thenReturn(null);
    }
    @Test
    void insertProducts_shouldInsertProducts() throws Exception {
        ProductSeeder productSeeder = new ProductSeeder(fireBaseService);

        Map<String, Product> products = new HashMap<>();
        products.put("uid1", new Product("name1", "type1", "10.00"));
        products.put("uid2", new Product("name2", "type2", "20.00"));
        products.put("uid3", new Product("name3", "type3", "30.00"));

        DatabaseReference ref = Mockito.mock(DatabaseReference.class);
        when(firebaseDatabase.getReference(DBReferences.PRODUCTS.getName())).thenReturn(ref);
        when(ref.child(anyString())).thenReturn(databaseReference);
        when(databaseReference.setValueAsync(any(Product.class))).thenReturn(apiFuture);


        productSeeder.insertProducts(products);

        verify(ref, times(products.size())).child(anyString());
        verify(databaseReference, times(products.size())).setValueAsync(any());
        verify(apiFuture, times(products.size())).get(anyLong(), any(TimeUnit.class));
    }

    @Test
    void generateProducts(){
        ProductSeeder productSeeder = new ProductSeeder(fireBaseService);

        DatabaseReference ref = Mockito.mock(DatabaseReference.class);
        when(firebaseDatabase.getReference(DBReferences.PRODUCTS.getName())).thenReturn(ref);
        when(ref.child(anyString())).thenReturn(databaseReference);
        when(databaseReference.setValueAsync(any(Product.class))).thenReturn(apiFuture);
        Set<String> productTypesKey = new HashSet<>(Arrays.asList("type1", "type2"));

        when(fireBaseService.getKeys(DBReferences.PRODUCT_TYPES.getName())).thenReturn(productTypesKey);

        Assertions.assertEquals(10, productSeeder.generateProducts(10));

    }

}