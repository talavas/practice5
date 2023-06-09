package shpp.level3.seed;
import com.google.api.core.ApiFuture;
import com.google.firebase.database.*;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.level3.model.Product;
import shpp.level3.util.DBReferences;
import shpp.level3.util.FireBaseService;
import shpp.level3.util.RandomGenerator;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ProductSeeder {
    private final Logger logger = LoggerFactory.getLogger("console");
    private final FireBaseService fireBaseService;
    private final AtomicInteger productCounter = new AtomicInteger(0);
    StopWatch timer = new StopWatch();
    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();


    public ProductSeeder(FireBaseService fireBaseService) {
        this.fireBaseService = fireBaseService;

    }

    public Set<String> getProductTypeKeys() {
        CountDownLatch done = new CountDownLatch(1);
        DatabaseReference dbRef = fireBaseService.getDb().getReference(DBReferences.PRODUCT_TYPES.getName());
        final Set<String> keys = new HashSet<>();
        dbRef.addValueEventListener(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                if (dataSnapshot.exists()) {
                    Map<String, Object> types = (Map<String, Object>) dataSnapshot.getValue();
                    keys.addAll(types.keySet());
                }
                    done.countDown();
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {
                logger.debug("Canceled read product-types.");
                done.countDown();
            }
        });
        try {
            done.await();
        } catch (InterruptedException e) {
            logger.error("Interrupt insert");
           Thread.currentThread().interrupt();
        }
        logger.debug("Keys = {}", keys);
        return keys;
    }
    private String getRandomProductTypeUid(List<String> productTypes) {
        logger.debug("Get random uid type");
        return productTypes.get(RandomGenerator.getRandom().nextInt(productTypes.size()));
    }

    public void generateProducts(int count, Set<String> productTypes) {
        logger.info("Generate {} products.", count);
        timer.reset();
        timer.start();
        Map<String, Product> products = new HashMap<>();

        List<String> productTypesList = new ArrayList<>(productTypes);
        while (productCounter.get() < count){
            String uid = UUID.randomUUID().toString();
            String name = RandomGenerator.generateRandomString();
            String type = getRandomProductTypeUid(productTypesList);
            String price = String.format("%.2f", (RandomGenerator.getRandom().nextDouble() * 100)).replace(",", ".");
            Product product = new Product(name, type, price);
            logger.debug("Generate product: {}", product);
            if(isValidProduct(product)){
                products.put(uid, product);
                productCounter.incrementAndGet();
            }
            if (products.size() % 100 == 0) {
                insertProducts(products);
                products = new HashMap<>();
            }
        }
        if(products.size() > 0){
            insertProducts(products);
            products.clear();
        }

        logger.info("Insert {} products", productCounter.get());
        logger.info("RPS = {}.", ((double) productCounter.get() / timer.getTime(TimeUnit.MILLISECONDS)) * 1000);
    }

    public void insertProducts(Map<String, Product> products) {
        DatabaseReference ref = fireBaseService.getDb().getReference("products");
        for( Map.Entry<String, Product> product : products.entrySet()){

            ApiFuture<Void> future = ref.child(product.getKey()).setValueAsync(product.getValue());

            try {
                future.get(10, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                logger.error("Error insert products", e);
                Thread.currentThread().interrupt();
            }
        }


    }
    protected boolean isValidProduct(Product product) {
        Set<ConstraintViolation<Product>> violations = validator.validate(product);
        return violations.isEmpty();
    }
}
