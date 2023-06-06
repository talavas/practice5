package shpp.level3.model;

import com.google.firebase.database.*;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.level3.util.FireBaseService;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class StoreDataRetriever {
        private final Logger logger = LoggerFactory.getLogger(StoreDataRetriever.class);
        private final FireBaseService fireBaseService;
        StopWatch timer = new StopWatch();

        public StoreDataRetriever(FireBaseService fireBaseService) {
            this.fireBaseService = fireBaseService;
        }

        public void retrieveProductTypeDetails(String productTypeName) {
            logger.info("Пошук магазина з товарами: {}", productTypeName);
            timer.start();
            String uid = getProductTypeUID(productTypeName);
            String storeUid = getStoreWithMaxQuantity(uid);
            getStoreDetails(storeUid);
            storeUid(uid);
            logger.info("Час пошуку = {}ms", timer.getTime(TimeUnit.MILLISECONDS));
        }

        private String getProductTypeUID(String productTypeName) {
            CountDownLatch done = new CountDownLatch(1);
            DatabaseReference dbRef = fireBaseService.getDb().getReference();
            StringBuilder uid = new StringBuilder();
            dbRef.child("product-types").addListenerForSingleValueEvent(new ValueEventListener() {
                @Override
                public void onDataChange(DataSnapshot dataSnapshot) {
                    logger.info("Datasnapshot = {}", dataSnapshot);
                    if (dataSnapshot.exists()) {
                        for (DataSnapshot childSnapshot : dataSnapshot.getChildren()) {
                            String key = childSnapshot.getKey();
                            String name = childSnapshot.child("name").getValue(String.class);
                            if (name.equals(productTypeName)) {
                                uid.append(key);
                                logger.info("Product type UID = {}", key);
                                done.countDown();
                                return; // Exit the loop after finding the matching product type
                            }
                        }
                        logger.info("Product type not found");
                    } else {
                        logger.info("No product types found");
                    }
                    done.countDown();
                }

                @Override
                public void onCancelled(DatabaseError databaseError) {
                    logger.error("Failed to retrieve product types");
                    done.countDown();
                }
            });
            try {
                done.await();
            } catch (InterruptedException e) {
                logger.error("Interrupt insert");
                e.printStackTrace();
            }
            return uid.toString();
        }

        private String getStoreWithMaxQuantity(String productTypeUID) {
            CountDownLatch done = new CountDownLatch(1);
            StringBuilder uid = new StringBuilder();
            DatabaseReference dbRef = fireBaseService.getDb().getReference();
            Query query = dbRef.child("inventory-by-type")
                    .child(productTypeUID)
                    .child("stores")
                    .orderByValue()
                    .limitToLast(1);
            logger.info("Query path = {}", query.getPath());
            query.addListenerForSingleValueEvent(new ValueEventListener() {
                @Override
                public void onDataChange(DataSnapshot dataSnapshot) {
                    logger.info("snapshot = {}", dataSnapshot);
                    if (dataSnapshot.exists()) {
                        DataSnapshot dataSnapshot1 = dataSnapshot.getChildren().iterator().next();
                        String storeUID = dataSnapshot1.getKey();
                        Integer qty = dataSnapshot1.getValue(Integer.class);
                        logger.info("Store UID {} with max quantity = {}", storeUID, qty);
                        uid.append(storeUID);
                    } else {
                        logger.info("No stores found for the given product type");
                    }
                    done.countDown();
                }

                @Override
                public void onCancelled(DatabaseError databaseError) {
                    logger.error("Failed to retrieve store with max quantity");
                    done.countDown();
                }
            });
            try {
                done.await();
            } catch (InterruptedException e) {
                logger.error("Interrupt insert");
                e.printStackTrace();
            }
            return uid.toString();
        }

        private void getStoreDetails(String storeUID) {
            CountDownLatch done = new CountDownLatch(1);
            DatabaseReference dbRef = fireBaseService.getDb().getReference("stores").child(storeUID);
            dbRef.addListenerForSingleValueEvent(new ValueEventListener() {
                @Override
                public void onDataChange(DataSnapshot dataSnapshot) {
                    logger.info("datasnapShop = {}", dataSnapshot);
                    if (dataSnapshot.exists()) {
                        Map<String, Object> store = (Map<String, Object>) dataSnapshot.getValue();
                        logger.info("Store details:");
                        logger.info("Store Name: {}", store.get("name"));
                        logger.info("Store Address: {}", store.get("location"));
                        // You can access other store properties as well
                    } else {
                        logger.info("Store not found");
                    }
                    done.countDown();
                }

                @Override
                public void onCancelled(DatabaseError databaseError) {
                    logger.error("Failed to retrieve store details");
                    done.countDown();
                }
            });
            try {
                done.await();
            } catch (InterruptedException e) {
                logger.error("Interrupt insert");
                e.printStackTrace();
            }
        }

        private String storeUid(String typeUID){
            CountDownLatch done = new CountDownLatch(1);
            DatabaseReference dbRef = fireBaseService.getDb().getReference("inventory");
            StringBuffer str = new StringBuffer();
            Query query = dbRef.child("/product_type_uid").equalTo(typeUID).orderByChild("store_uid");

            query.addListenerForSingleValueEvent(new ValueEventListener() {
                @Override
                public void onDataChange(DataSnapshot dataSnapshot) {
                    logger.info("Snapshot = ", dataSnapshot);
                    done.countDown();
                }

                @Override
                public void onCancelled(DatabaseError databaseError) {
                    logger.info(databaseError.getMessage());
done.countDown();
                }
            });
            return str.toString();
        }
}
