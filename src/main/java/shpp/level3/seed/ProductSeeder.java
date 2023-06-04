package shpp.level3.seed;
import com.google.api.core.ApiFuture;
import com.google.firebase.database.*;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.level3.model.Product;
import shpp.level3.util.FireBaseService;
import shpp.level3.util.RandomGenerator;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ProductSeeder {
    private final Logger logger = LoggerFactory.getLogger(ProductSeeder.class);
    private final FireBaseService fireBaseService;

    private final AtomicInteger productCounter = new AtomicInteger(0);
    StopWatch timer = new StopWatch();


    public ProductSeeder(FireBaseService fireBaseService) {
        this.fireBaseService = fireBaseService;
    }

    public Set<String> getProductTypeKeys() {
        CountDownLatch done = new CountDownLatch(1);
        DatabaseReference dbRef = fireBaseService.getDb().getReference();
        final Set<String> keys = new HashSet<>();
        dbRef.child("product-types").addValueEventListener(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                if (dataSnapshot.exists()) {
                    Map<String, Object> types = (Map<String, Object>) dataSnapshot.getValue();
                    keys.addAll(types.keySet());
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
        logger.debug("Keys = {}", keys);
        return keys;
    }
    private String getRandomProductTypeUid(List<String> productTypes) {
        logger.debug("Get random uid type");
        return productTypes.get(RandomGenerator.getRandom().nextInt(productTypes.size()));
    }

    public void generateProducts(int count, Set<String> productTypes) {
        logger.debug("Generate products.");
        timer.reset();
        timer.start();
        Map<String, Product> products = new HashMap<>();

        List<String> productTypesList = new ArrayList<>(productTypes);

        for (int i = 0; i < count; i++) {
            logger.debug("For loop");
            String uid = UUID.randomUUID().toString();
            logger.debug("UID = {}", uid);
            String name = RandomGenerator.generateRandomString();
            logger.debug("name = {}", name);
            String type = getRandomProductTypeUid(productTypesList);
            logger.debug("type = {}", type);
            String price = String.format("%.2f", (RandomGenerator.getRandom().nextDouble() * 100)).replace(",", ".");
            logger.debug("price = {}", price);
            Product product = new Product(name, type, price);
            logger.debug("Generate product: {}", product);

            products.put(uid, product);
            if (products.size() >= 100 || i == count - 1) {
                insertProducts(products);
                products = new HashMap<>();
            }
        }
        logger.info("Insert {} products", productCounter.get());
        logger.info("RPS = {}.", ((double) productCounter.get() / timer.getTime(TimeUnit.MILLISECONDS)) * 1000);
    }

    public void insertProducts(Map<String, Product> products) {
        DatabaseReference ref = fireBaseService.getDb().getReference("products");
        for(String key: products.keySet()){

            ApiFuture<Void> future = ref.child(key).setValueAsync(products.get(key));
            productCounter.incrementAndGet();
            try {
                future.get(10, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }


    }
}
