package shpp.level3.seed;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.firestore.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.level3.model.Product;
import shpp.level3.util.FireBaseService;
import shpp.level3.util.RandomGenerator;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class ProductGenerator {
    private final Logger logger = LoggerFactory.getLogger(ProductGenerator.class);
    FireBaseService fireBaseService;
    private CollectionReference productsCollection;

    private CollectionReference productTypesCollection;

    private int batchSize;

    public ProductGenerator(FireBaseService fireBaseService) {
        this.fireBaseService = fireBaseService;
        this.productsCollection = fireBaseService.getCollection("products");
        this.productTypesCollection = fireBaseService.getCollection("product-types");
        this.batchSize = Integer.parseInt(fireBaseService.getConfig().getProperty("batch.size"));
    }
    private List<String> getProductTypes() {
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


    public void generateProducts(int count, int batchSize) {
        List<Product> products = generateRandomProducts(count);
        WriteBatch batch = fireBaseService.getFirestore().batch();

        List<ApiFuture<WriteResult>> futures = new ArrayList<>();
        int batchCount = 0;

        for (Product product : products) {
            DocumentReference documentReference = productsCollection.document();
            futures.add(documentReference.set(product));

            batchCount++;

            if (batchCount == batchSize) {
                // Виконуємо пакетну операцію для поточного пакету документів
                writeBatch(futures);

                // Очищаємо список майбутніх результатів та скидаємо лічильник пакету
                futures.clear();
                batchCount = 0;
            }
        }

        // Виконуємо останній пакет, якщо він не повний
        if (!futures.isEmpty()) {
            writeBatch(futures);
        }
    }
    private void writeBatch(List<ApiFuture<WriteResult>> futures) {
        try {
            List<WriteResult> results = ApiFutures.allAsList(futures).get();

            for (WriteResult result : results) {
               logger.debug("Product added to Firestore: {}",result.getUpdateTime());
            }
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error adding products to Firestore.",e);
        }
    }

    private List<Product> generateRandomProducts(int count) {
        List<Product> products = new ArrayList<>();
        List<String> productTypes = getProductTypes();
        Random random = new Random();

        for (int i = 0; i < count; i++) {
            // Генеруємо випадкові дані для продукту
            String name = RandomGenerator.generateRandomString();
            String type = RandomGenerator.getRandomStringFromList(productTypes);
            String price = String.format("%.2f", (random.nextDouble() * 100));

            // Створюємо об'єкт продукту і додаємо його до списку
            Product product = new Product(name, type, price);
            products.add(product);
        }

        return products;
    }
}
