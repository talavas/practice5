package shpp.level3.seed;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.WriteBatch;
import com.google.cloud.firestore.WriteResult;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.level3.util.FireBaseService;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class FirestoreSeeder {
    private final Logger logger = LoggerFactory.getLogger(FirestoreSeeder.class);
    FireBaseService fireBaseService;

    StopWatch timer = new StopWatch();

    int counter = 0;

    public FirestoreSeeder(FireBaseService fireBaseService) {
        this.fireBaseService = fireBaseService;
    }

    public void seed(String filename) {
        String collectionName = filename.substring(0, filename.lastIndexOf("."));
        //fireBaseService.deleteCollection(collectionName);
        timer.reset();
        timer.start();
        CollectionReference collection = fireBaseService.getCollection(collectionName);
        InputStream csvFileInputStream = getClass().getClassLoader().getResourceAsStream(filename);

        if (csvFileInputStream != null) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(csvFileInputStream))) {
                CSVFormat csvFormat = CSVFormat.DEFAULT.withFirstRecordAsHeader();
                CSVParser csvParser = csvFormat.parse(reader);
                List<CSVRecord> records = csvParser.getRecords();

                WriteBatch batch = fireBaseService.getFirestore().batch();
                timer.reset();
                timer.start();
                logger.info("Start reading rows from csv file {}.", filename);

                for (CSVRecord row : records) {
                    DocumentReference documentRef = collection.document();
                    batch.set(documentRef, row.toMap());
                    counter++;

                    // Вставити пакет, якщо досягнуто розміру пакету
                    if (batch.getMutationsSize() >= fireBaseService.getBatchSize()) {
                        logger.debug("Commit batch of rows.");
                        batch.commit().get();
                        logger.debug("Batch inserted.");
                        batch = fireBaseService.getFirestore().batch();
                    }
                }

                if (batch.getMutationsSize() > 0) {
                    logger.debug("Commit last batch.");
                    batch.commit().get();
                    logger.debug("Last batch inserted.");
                }
                logger.info("Collection {} from {} documents created",
                        collectionName,
                        counter);
                logger.info("Collection {} created, RPS = {}."
                        , collectionName
                , ((double) counter / timer.getTime(TimeUnit.MILLISECONDS))*1000);

            } catch (IOException e) {
               logger.error("Can't read file.", e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
