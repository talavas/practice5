package shpp.level3;

import shpp.level3.model.StoreDataRetriever;
import shpp.level3.seed.FirebaseSeederFromCSV;
import shpp.level3.seed.InventorySeeder;
import shpp.level3.seed.InventoryUpdater;
import shpp.level3.seed.ProductSeeder;
import shpp.level3.util.Config;
import shpp.level3.util.FireBaseService;

import java.util.concurrent.CompletableFuture;

public class Main {
    public static final String PRODUCT_TYPES_CSV = "product-types.csv";
    public static final String STORES_CSV = "stores.csv";
    public static final String APP_PROPERTIES = "app.properties";

    public static void main(String[] args)  {
        Config config = new Config(APP_PROPERTIES);
        FireBaseService fireBaseService = new FireBaseService(config);
        String productType = System.getProperty("type");
        if(productType == null){
//            fireBaseService.clearDatabase();
//
//            FirebaseSeederFromCSV seeder = new FirebaseSeederFromCSV(fireBaseService);
//            seeder.seed(PRODUCT_TYPES_CSV);
//            seeder.seed(STORES_CSV);
//
//            ProductSeeder productSeeder = new ProductSeeder(fireBaseService);
//            productSeeder.generateProducts(Integer.parseInt(config.getProperty("products.count")));
//
//            InventorySeeder inventorySeeder = new InventorySeeder(fireBaseService);
//            CompletableFuture<Void> inventorySeedFuture = CompletableFuture.runAsync(inventorySeeder::generateInventory);

            InventoryUpdater inventoryUpdater = new InventoryUpdater(fireBaseService);

            CompletableFuture<Void> updateInventoryFuture = CompletableFuture.runAsync(inventoryUpdater::update);

            updateInventoryFuture.join();

            StoreDataRetriever store = new StoreDataRetriever(fireBaseService);
            store.retrieveProductTypeDetails("Клей");
            store.retrieveProductTypeDetails("Фурнітура для дверей");
        }else{
            StoreDataRetriever store = new StoreDataRetriever(fireBaseService);
            store.retrieveProductTypeDetails(productType);
            store.retrieveProductTypeDetails(productType);
        }

    }

}