package shpp.level3;

import shpp.level3.model.StoreDataRetriever;
import shpp.level3.seed.FirestoreSeeder;
import shpp.level3.seed.InventorySeeder;
import shpp.level3.seed.InventoryUpdater;
import shpp.level3.seed.ProductSeeder;
import shpp.level3.util.Config;
import shpp.level3.util.FireBaseService;
import shpp.level3.util.FirebaseRulesManager;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class Main {
    public static void main(String[] args) throws Exception {
        Config config = new Config("app.properties");
        FireBaseService fireBaseService = new FireBaseService(config);
        fireBaseService.clearDatabase();

        FirestoreSeeder seeder = new FirestoreSeeder(fireBaseService);
        seeder.seed("product-types.csv");
        seeder.seed("stores.csv");

        ProductSeeder productSeeder = new ProductSeeder(fireBaseService);
        Set<String> productTypeKeys = productSeeder.getProductTypeKeys();
        productSeeder.generateProducts(Integer.parseInt(config.getProperty("products.count")), productTypeKeys);

        InventorySeeder inventorySeeder = new InventorySeeder(fireBaseService);
        CompletableFuture<Void> inventorySeedFuture = CompletableFuture.runAsync(inventorySeeder::generateInventory);

        FirebaseRulesManager rulesManager = new FirebaseRulesManager(config);
        rulesManager.setNewRules();
        InventoryUpdater inventoryUpdater = new InventoryUpdater(fireBaseService);

        CompletableFuture<Void> updateInventoryFuture = inventorySeedFuture
                .thenCompose(voidResult -> CompletableFuture.runAsync(inventoryUpdater::update));

        updateInventoryFuture.join();

        StoreDataRetriever store = new StoreDataRetriever(fireBaseService);
        store.retrieveProductTypeDetails("Клей");

    }

}