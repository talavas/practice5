package shpp.level3.seed;

import com.google.api.core.ApiFuture;
import com.google.firebase.database.DatabaseReference;
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
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class FirestoreSeeder {
    private final Logger logger = LoggerFactory.getLogger(FirestoreSeeder.class);
    FireBaseService fireBaseService;



    int counter = 0;

    public FirestoreSeeder(FireBaseService fireBaseService) {
        this.fireBaseService = fireBaseService;
    }

    public void seed(String filename) throws IOException {

        String collectionName = filename.substring(0, filename.lastIndexOf("."));
        StopWatch timer = new StopWatch();

        InputStream csvFileInputStream = getClass().getClassLoader().getResourceAsStream(filename);

        if (csvFileInputStream != null) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(csvFileInputStream))) {
                CSVFormat csvFormat = CSVFormat.DEFAULT.withFirstRecordAsHeader();
                CSVParser csvParser = csvFormat.parse(reader);
                List<CSVRecord> records = csvParser.getRecords();


                logger.info("Start reading rows from csv file {}.", filename);
                timer.reset();
                timer.start();
                DatabaseReference ref = fireBaseService.getDb().getReference(collectionName);
                Map<String, Object> types = new HashMap<>();
                for (CSVRecord row : records) {
                    types.put(UUID.randomUUID().toString(), row.toMap());
                    counter++;
                }
                ApiFuture<Void> future = ref.setValueAsync(types);
                future.get(10, TimeUnit.SECONDS);

                logger.info("Collection {} from {} documents created",
                        collectionName,
                        counter);
                logger.info("Time = {} ms", timer.getTime(TimeUnit.MILLISECONDS));
                logger.info("Collection {} created, RPS = {}."
                        , collectionName
                , ((double) counter / timer.getTime(TimeUnit.MILLISECONDS))*1000);
                counter = 0;

            } catch (IOException e) {
               logger.error("Can't read file.", e);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
