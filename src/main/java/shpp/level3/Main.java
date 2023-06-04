package shpp.level3;

import shpp.level3.model.ProductTypeDetailsRetriever;
import shpp.level3.seed.FirestoreSeeder;
import shpp.level3.seed.InventorySeeder;
import shpp.level3.seed.ProductSeeder;
import shpp.level3.seed.ProductsCollectionGen;
import shpp.level3.util.Config;
import shpp.level3.util.FireBaseService;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class Main {
    public static void main(String[] args) throws Exception {
        Config config = new Config("app.properties");
        FireBaseService fireBaseService = new FireBaseService(config);

//        FirestoreSeeder seeder = new FirestoreSeeder(fireBaseService);
//       seeder.seed("product-types.csv");
//       seeder.seed("stores.csv");
//        ProductSeeder productSeeder = new ProductSeeder(fireBaseService);
//        Set<String> productTypeKeys = productSeeder.getProductTypeKeys();
//        productSeeder.generateProducts(10000, productTypeKeys);
//        InventorySeeder inventorySeeder = new InventorySeeder(fireBaseService);
//        inventorySeeder.generateInventory();
    ProductTypeDetailsRetriever store = new ProductTypeDetailsRetriever(fireBaseService);
        store.retrieveProductTypeDetails("Будівельні матеріали");
       //ProductsCollectionGen productsGenerator = new ProductsCollectionGen(fireBaseService);
       //productsGenerator.generateDocuments(1000);
       //fireBaseService.closeFirestore();
    }

}