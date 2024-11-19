package verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import models.SSTable;
import services.SSTableManager;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Optional;

/**
 * Explanation:
 *
 * 	1.	Dynamic Configuration via Vert.x Config:
 * 	•	Reads parameters like the SSTable directory, Bloom filter size, hash count, and max keys per SSTable from the configuration object (config()).
 * 	2.	Event Bus Address Changes:
 * 	•	sstable.put: For handling PUT requests (batch writes).
 * 	•	sstable.get: For handling GET requests (single key reads).
 * 	3.	Error Handling:
 * 	•	Added detailed logging for exceptions during read and write operations.
 * 	•	Properly replies with failure messages if operations fail (message.fail).
 * 	4.	Aligned with Updated SSTableManager:
 * 	•	Integrated with the corrected SSTableManager class using its new put and get methods.
 * 	5.	Batch Writes:
 * 	•	The handleWriteRequest method loops through the map of key-value pairs and writes them individually to the active SSTable.
 * 	6.	Single Reads:
 * 	•	The handleReadRequest method fetches the value for the requested key using SSTableManager and responds with the value or "NOT_FOUND".
 */
public class FilePersistenceVerticle extends AbstractVerticle {
    private SSTableManager sstableManager;

    @Override
    public void start(Promise<Void> startPromise) {
        try {
            // Initialize SSTableManager with configurations
            String directory = config().getString("sstableDirectory", "data");
            int bloomFilterSize = config().getInteger("bloomFilterSize", 1000);
            int bloomHashCount = config().getInteger("bloomHashCount", 3);
            int maxKeysPerSSTable = config().getInteger("maxKeysPerSSTable", 1000);

            sstableManager = new SSTableManager(directory, bloomFilterSize, bloomHashCount, maxKeysPerSSTable, vertx);

            // Register event bus consumers
            registerEventBusHandlers();

            startPromise.complete();
            System.out.println("FilePersistenceVerticle started successfully.");
        } catch (Exception e) {
            startPromise.fail(e);
            System.err.println("Failed to start FilePersistenceVerticle: " + e.getMessage());
        }
    }

    private void registerEventBusHandlers() {
        // Handle PUT requests to write key-value pairs
        vertx.eventBus().consumer("sstable.write", this::handleWriteRequest);

        // Handle GET requests to retrieve key values
        vertx.eventBus().consumer("sstable.read", this::handleReadRequest);
    }

    private void handleWriteRequest(Message<Object> message) {
        try {
            @SuppressWarnings("unchecked") Map<String, String> data = (Map<String, String>) message.body();

            // Perform batch write
            for (var entry : data.entrySet()) {
                sstableManager.put(entry.getKey(), entry.getValue());

                //Map.Entry<String, SSTable> tuple = new AbstractMap.SimpleEntry<>(key, activeSSTable);
                //vertx.eventBus().send("bloomFilter.add", tuple);
            }

            message.reply("Write successful");
        } catch (Exception e) {
            System.err.println("Error during write operation: " + e.getMessage());
            message.fail(500, e.getMessage());
        }
    }

    private void handleReadRequest(Message<Object> message) {
        try {
            String key = (String) message.body();

            // Retrieve value from SSTableManager
            Optional<String> value = sstableManager.get(key);

            // Reply with the value or NOT_FOUND
            message.reply(value.orElse("NOT_FOUND"));
        } catch (Exception e) {
            System.err.println("Error during read operation: " + e.getMessage());
            message.fail(500, e.getMessage());
        }
    }
}