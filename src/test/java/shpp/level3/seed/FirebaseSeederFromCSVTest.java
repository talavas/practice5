package shpp.level3.seed;

import com.google.api.core.ApiFuture;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import shpp.level3.util.FireBaseService;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class FirebaseSeederFromCSVTest {
    @Mock
    private FireBaseService fireBaseService;

    @Mock
    private ApiFuture<Void> apiFuture;
    @Mock
    private FirebaseDatabase firebaseDatabase;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }
    @Test
    void seed_shouldSeedCollectionFromCSV() throws Exception {
        String filename = "data.csv";
        FirebaseSeederFromCSV firebaseSeeder = new FirebaseSeederFromCSV(fireBaseService);

        DatabaseReference ref = mock(DatabaseReference.class);
        when(fireBaseService.getDb()).thenReturn(firebaseDatabase);
        when(firebaseDatabase.getReference(anyString())).thenReturn(ref);
        when(ref.setValueAsync(any())).thenReturn(apiFuture);
        when(apiFuture.get(anyLong(), any())).thenReturn(null);

        firebaseSeeder.seed(filename);

        verify(firebaseDatabase, times(1)).getReference(anyString());
        verify(apiFuture, times(1)).get(anyLong(), any());
        Assertions.assertEquals(2, firebaseSeeder.counter);
    }
}