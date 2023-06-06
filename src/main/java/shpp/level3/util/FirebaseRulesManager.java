package shpp.level3.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class FirebaseRulesManager {
    private final Logger logger = LoggerFactory.getLogger("console");
    private final String endpoint;

    public FirebaseRulesManager(Config config) {
        this.endpoint = String.format("https://%s.firebaseio.com/.settings/rules.json?auth=%s",
                config.getProperty("project.id"), config.getProperty("firebase.secret"));
    }


    public void setNewRules() throws Exception {
        // Updated rules for the index
        String newRules = "{ \"rules\": { \"inventory\": { \"$uid\": { \"product_type_id\": {\".indexOn\": \".value\"} } } } }";

        // Create HttpClient
        HttpClient httpClient = HttpClient.newHttpClient();

        // Build the request
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(endpoint))
                .header("Content-Type", "application/json")
                .method("PUT", HttpRequest.BodyPublishers.ofString(newRules))
                .build();

        // Send the request
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        // Get the response status code
        int statusCode = response.statusCode();

        // Get the response body
        String responseBody = response.body();

        // Get the response headers
        HttpHeaders headers = response.headers();

        // Print the response information
        logger.info("Response Status Code: {}", statusCode);
        logger.info("Response Body: {}", responseBody);
        logger.info("Response Headers: {}",headers);
    }
}
