package shpp.level3;

import shpp.level3.seed.FirestoreSeeder;
import shpp.level3.seed.ProductsCollectionGen;
import shpp.level3.util.Config;
import shpp.level3.util.FireBaseService;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws Exception {
        Config config = new Config("app.properties");
        FireBaseService fireBaseService = new FireBaseService(config);

        //FirestoreSeeder seeder = new FirestoreSeeder(fireBaseService);
       //seeder.seed("product-types.csv");
       ProductsCollectionGen productsGenerator = new ProductsCollectionGen(fireBaseService);
       productsGenerator.generateDocuments(1000);
       fireBaseService.closeFirestore();
    }

}