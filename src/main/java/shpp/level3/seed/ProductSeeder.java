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

    private final AtomicInteger noValidProduct = new AtomicInteger(0);
    StopWatch timer = new StopWatch();
    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();


    public ProductSeeder(FireBaseService fireBaseService) {
        this.fireBaseService = fireBaseService;

    }

    public int generateProducts(int count) {
        logger.info("Generating {} products.", count);
        timer.reset();
        timer.start();
        Map<String, Product> products = new HashMap<>();
        Set<String> productTypesKeys = fireBaseService.getKeys(DBReferences.PRODUCT_TYPES.getName());

        List<String> productTypesList = new ArrayList<>(productTypesKeys);
        while (productCounter.get() < count){
            String uid = UUID.randomUUID().toString();
            String name = RandomGenerator.generateRandomString();
            String typeUid = RandomGenerator.getRandomKeyFromKeysList(productTypesList);
            String price = String.format("%.2f", (RandomGenerator.getRandom().nextDouble() * 100)).replace(",", ".");
            Product product = new Product(name, typeUid, price);
            logger.debug("Generate product: {}", product);
            if(isValidProduct(product)){
                products.put(uid, product);
                productCounter.incrementAndGet();
            }else{
                noValidProduct.incrementAndGet();
            }
            if (products.size() % 100 == 0) {
                logger.info("Generated {} products.", productCounter.get());
                insertProducts(products);
                products = new HashMap<>();
            }
        }
        if(products.size() > 0){
            logger.info("Generated {} products.", productCounter.get());
            insertProducts(products);
            products.clear();
        }

        logger.info("Generated no valid products = {}", noValidProduct.get());
        logger.info("Insert {} valid products", productCounter.get());
        logger.info("RPS = {}.", ((double) productCounter.get() / timer.getTime(TimeUnit.MILLISECONDS)) * 1000);
        return productCounter.get();
    }

    public void insertProducts(Map<String, Product> products) {
        DatabaseReference ref = fireBaseService.getDb().getReference(DBReferences.PRODUCTS.getName());
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
