package verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import models.SSTable;

import java.util.List;
import java.util.Map;

public class BloomFilterUpdateVerticle extends AbstractVerticle {

    @Override
    public void start(Promise<Void> promise) {
        try {
            registerEventBusHandlers();
            promise.complete();
            System.out.println("BloomFilterUpdateVerticle started successfully.");
        } catch (Exception e) {
            promise.fail(e);
            System.err.println("Failed to start BloomFilterUpdateVerticle: " + e.getMessage());
        }
    }

    private void registerEventBusHandlers() {
        vertx.eventBus().consumer("bloomFilter.add", this::handleBloomFilterAdd);

    }

    private void handleBloomFilterAdd(Message<Object> message) {

        try {
            Map.Entry<String, SSTable> tuple = (Map.Entry<String, SSTable>) (message.body());
            String key = tuple.getKey();
            SSTable ssTable = tuple.getValue();

            ssTable.getBloomFilter().add(key);

            System.out.println("Bloom Filter updated successfully.");
            message.reply("Bloom Filter updated");

        } catch (Exception e) {
            System.err.println("Error during Bloom Filter update operation: " + e.getMessage());
            message.fail(500, e.getMessage());
        }


    }
}
