package shpp.level3.model;

import com.google.firebase.database.*;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.level3.util.FireBaseService;

import java.util.Map;
import java.util.concurrent.*;


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
            CompletableFuture<String> getTypeUid = new CompletableFuture<>();
            DatabaseReference dbRef = fireBaseService.getDb().getReference();

            Query query = dbRef.child("product-types").orderByChild("name").equalTo(productTypeName);
            query.addListenerForSingleValueEvent(new ValueEventListener() {
                @Override
                public void onDataChange(DataSnapshot dataSnapshot) {
                    logger.debug("Data snapshot = {}", dataSnapshot);
                    if (dataSnapshot.exists()) {
                        getTypeUid.complete(dataSnapshot.getChildren().iterator().next().getKey());
                    } else {
                        getTypeUid.complete(null);
                        logger.info("Product type for name {} not found in database.", productTypeName);
                    }
                }

                @Override
                public void onCancelled(DatabaseError databaseError) {
                    logger.error("Failed to retrieve product types");
                    getTypeUid.complete(null);
                }
            });
            String typeUid = null;
            try {
                typeUid = getTypeUid.get(10, TimeUnit.SECONDS);
                logger.debug("Receive typeUid = {} for {}ms",typeUid, timer.getTime(TimeUnit.MILLISECONDS));
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                logger.error("Can't receive products list", e);
                Thread.currentThread().interrupt();
            }

            return typeUid;
        }

        private String getStoreWithMaxQuantity(String productTypeUID) {
            CompletableFuture<String> getStoreUid = new CompletableFuture<>();
            DatabaseReference dbRef = fireBaseService.getDb().getReference();
            Query query = dbRef.child("inventory-by-type")
                    .child(productTypeUID)
                    .child("stores")
                    .orderByValue()
                    .limitToLast(1);
            logger.debug("Query path = {}", query.getPath());
            query.addListenerForSingleValueEvent(new ValueEventListener() {
                @Override
                public void onDataChange(DataSnapshot dataSnapshot) {
                    logger.debug("Query = {}, snapshot = {}", query, dataSnapshot);
                    if (dataSnapshot.exists()) {
                        DataSnapshot storeSnapShot = dataSnapshot.getChildren().iterator().next();
                        getStoreUid.complete(storeSnapShot.getKey());
                        Integer qty = storeSnapShot.getValue(Integer.class);
                        logger.info("Знайдено магазин з UID {} з набільшою кількістю = {}", storeSnapShot.getKey(), qty);
                    } else {
                        logger.info("Немає магазинів де є товари типу з uid = {}", productTypeUID);
                        getStoreUid.complete(null);
                    }
                }

                @Override
                public void onCancelled(DatabaseError databaseError) {
                    logger.error("Запит на пошук магазину скасовано, причина: ", databaseError.toException());
                    getStoreUid.complete(null);
                }
            });
            String storeUid = null;

            try {
                storeUid = getStoreUid.get(10, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                logger.error("Can't receive store uid.", e);
                Thread.currentThread().interrupt();
            }
            return storeUid;
        }

        private void getStoreDetails(String storeUID) {
            CompletableFuture<String> getStoreDetails = new CompletableFuture<>();
            DatabaseReference dbRef = fireBaseService.getDb().getReference("stores").child(storeUID);
            dbRef.addListenerForSingleValueEvent(new ValueEventListener() {
                @Override
                public void onDataChange(DataSnapshot dataSnapshot) {
                    logger.debug("Store details snap shot = {}", dataSnapshot);
                    if (dataSnapshot.exists()) {
                        Map<String, Object> store = (Map<String, Object>) dataSnapshot.getValue();
                        logger.info("Деталі магазину:");
                        logger.info("Назва: {}", store.get("name"));
                        logger.info("Адреса: {}", store.get("location"));
                    } else {
                        logger.info("Не знайдено деталей магазину.");
                    }
                    getStoreDetails.complete(null);
                }

                @Override
                public void onCancelled(DatabaseError databaseError) {
                    logger.error("Failed to retrieve store details");
                   getStoreDetails.complete(null);
                }
            });

            try {
               getStoreDetails.get(10, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                logger.error("Can't receive store details.", e);
                Thread.currentThread().interrupt();
            }
        }
}
