package shpp.level3.seed;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.*;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.level3.model.Product;
import shpp.level3.util.FireBaseService;
import shpp.level3.util.RandomGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;


public class ProductsCollectionGen {
    private final Logger logger = LoggerFactory.getLogger(ProductsCollectionGen.class);
    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    private final FireBaseService fireBaseService;
    protected AtomicInteger availableThreads;

   private ExecutorService executor;

   StopWatch timer = new StopWatch();
   private CollectionReference productsCollection;

   private CollectionReference productTypesCollection;

    public ProductsCollectionGen(FireBaseService fireBaseService) {
        this.fireBaseService = fireBaseService;
        this.executor = Executors.newFixedThreadPool(
                Integer.parseInt(fireBaseService.getConfig().getProperty("threads"))
        );
        this.availableThreads = new AtomicInteger(
                Integer.parseInt(fireBaseService.getConfig().getProperty("threads"))
        );
        this.productsCollection = fireBaseService.getCollection("products");
        this.productTypesCollection = fireBaseService.getCollection("product-types");

    }
    private List<String> getProductTypes() {
        logger.info("Get product types");
        List<String> productTypes = new ArrayList<>();
        ApiFuture<QuerySnapshot> future = productTypesCollection.get();

        try {
            QuerySnapshot snapshot = future.get();
            List<QueryDocumentSnapshot> documents = snapshot.getDocuments();

            for (QueryDocumentSnapshot document : documents) {
                String type = document.getString("name");
                productTypes.add(type);
            }
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Can't execute insert.", e);
            Thread.currentThread().interrupt();
        }

        return productTypes;
    }

    public int generateDocuments(int count){
        timer.reset();
        timer.start();
        AtomicInteger invalidProduct = new AtomicInteger();
        AtomicInteger validProduct = new AtomicInteger();
        List<Product> productBatch = new ArrayList<>(fireBaseService.getBatchSize());
        List<String> productTypes = getProductTypes();
        Stream.generate(() ->RandomGenerator.getRandomProduct(productTypes))
                .takeWhile(ignored -> validProduct.get() < count)
                .forEach(product -> {
                    if (isValidProduct(product)) {
                        productBatch.add(product);
                        validProduct.incrementAndGet();
                    } else {
                        invalidProduct.incrementAndGet();
                    }
                    if (productBatch.size() >= fireBaseService.getBatchSize()) {
                        while (availableThreads.get() == 0) {
                            try {
                                logger.debug("Product stream generator pause");
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                            List<Product> batch = new ArrayList<>(productBatch);
                            productBatch.clear();
                            submitTask(batch);
                    }
                });
                    if(!productBatch.isEmpty()){
                        submitTask(productBatch);
                    }
                    executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            logger.info("All batches inserted successfully.");
            logger.info("Invalid product = {}", invalidProduct.get());
            logger.info("Insert valid product = {}", validProduct.get());
            long time = timer.getTime(TimeUnit.MILLISECONDS);
            logger.info("Time execution = {}", time);
            logger.info("Insert product RPS={}", (validProduct.doubleValue() / time) * 1000);

        } catch (InterruptedException e) {
            logger.error("Error occurred while waiting for the threads to finish.",e);
            Thread.currentThread().interrupt();
        }
        return validProduct.get();
    }


    private void submitTask(List<Product> batch) {
        availableThreads.decrementAndGet();

        executor.submit(() -> {
            logger.info("Thread {} submit batch {} to insert", Thread.currentThread().getName(), batch.size());
            insertBatch(batch);
            availableThreads.incrementAndGet();
            logger.info("Thread {} insert batch, available threads", Thread.currentThread().getName());

        });
    }

    private void insertBatch(List<Product> products) {
        logger.info("Inserting batch");
        WriteBatch batch = fireBaseService.getFirestore().batch();
        for (Product product : products) {
            DocumentReference documentReference = productsCollection.document();
            batch.set(documentReference, product.toMap());
        }

        batch.commit();

            logger.info("Batch commit");
            logger.debug("Products added to Firestore in batch.");
    }


    protected boolean isValidProduct(Product product) {
        Set<ConstraintViolation<Product>> violations = validator.validate(product);
        return violations.isEmpty();
    }




}
