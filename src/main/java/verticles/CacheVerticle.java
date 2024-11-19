package verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;

import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class CacheVerticle extends AbstractVerticle {

    private ConcurrentHashMap<String,String> cache;

    @Override
    public void start(Promise<Void> promise) {
        try {
            cache = new ConcurrentHashMap<>();
            System.out.println("New ConcurrentHashMap Cache created at: " + new Date().getTime());
            registerEventBusHandlers();
            promise.complete();
            System.out.println("CacheVerticle started successfully.");
        } catch (Exception e) {
            promise.fail(e);
            System.err.println("Failed to start CacheVerticle: " + e.getMessage());
        }
    }

    private void registerEventBusHandlers() {
        // Handle PUT requests to write key-value pairs
        vertx.eventBus().consumer("cache.write", this::handleCacheWriteRequest);

        // Handle GET requests to retrieve key values
        vertx.eventBus().consumer("cache.read", this::handleCacheReadRequest);
    }

    private void handleCacheReadRequest(Message<Object> message) {

        try {
            String key = (String) message.body();

            // Retrieve value from SSTableManager
            Optional<String> value = Optional.ofNullable(cache.get(key));

            // Reply with the value or NOT_FOUND
            message.reply(value.orElse("NOT_FOUND"));

        } catch (Exception e) {
            System.err.println("Error during read operation: " + e.getMessage());
            message.fail(500, e.getMessage());
        }


    }

    private void handleCacheWriteRequest(Message<Object> message) {
        try {
            @SuppressWarnings("unchecked") Map<String, String> data = (Map<String, String>) message.body();

            // Perform batch write
            for (var entry : data.entrySet()) {
                cache.put(entry.getKey(), entry.getValue());
            }

            message.reply("Cache Write successful");
        } catch (Exception e) {
            System.err.println("Error during write operation: " + e.getMessage());
            message.fail(500, e.getMessage());
        }
    }
}
