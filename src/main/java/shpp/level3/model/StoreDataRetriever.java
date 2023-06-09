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
            String storeUid = null;
            if(uid != null){
                storeUid = getStoreWithMaxQuantity(uid);
            }else{
                logger.info("Не знайдено типу товара {}", productTypeName);
            }

            if(storeUid != null){
                getStoreDetails(storeUid);
            }else{
                logger.info("Магазин не знайдено");
            }


            logger.info("Час пошуку = {}ms", timer.getTime(TimeUnit.MILLISECONDS));
            timer.stop();
            timer.reset();
        }

        private String getProductTypeUID(String productTypeName) {
            CountDownLatch done = new CountDownLatch(1);
            DatabaseReference dbRef = fireBaseService.getDb().getReference();
            StringBuilder uid = new StringBuilder();

            Query query = dbRef.child("product-types").orderByChild("name").equalTo(productTypeName);
            query.addListenerForSingleValueEvent(new ValueEventListener() {
                @Override
                public void onDataChange(DataSnapshot dataSnapshot) {
                    logger.debug("Data snapshot = {}", dataSnapshot);
                    if (dataSnapshot.exists()) {
                        String key = dataSnapshot.getChildren().iterator().next().getKey();
                        uid.append(key);
                        logger.info("Product type UID = {}", key);
                        done.countDown();
                    } else {
                        logger.info("Product type for {} not found in database.", productTypeName);
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
                logger.error("Firebase operation interrupt. ", e);
                Thread.currentThread().interrupt();
            }
            return uid.length() > 0 ? uid.toString() : null;
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
                    logger.debug("Query = {}, snapshot = {}", query, dataSnapshot);
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
                logger.error("Interrupt insert", e);
                Thread.currentThread().interrupt();
            }
            return uid.length() > 0 ? uid.toString() : null;
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
                        logger.info("Деталі магазину:");
                        logger.info("Назва: {}", store.get("name"));
                        logger.info("Адреса: {}", store.get("location"));
                    } else {
                        logger.info("Не знайдено деталей магазину.");
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
                logger.error("Interrupt insert", e);
                Thread.currentThread().interrupt();
            }
        }
}
