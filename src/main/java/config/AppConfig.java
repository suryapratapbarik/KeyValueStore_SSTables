package config;

import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import lombok.Data;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

@Data
public class AppConfig {
    private JsonObject config;

    // Cache Configuration
    private int cacheVerticleInstances;
    private int cacheSize;
    private String evictionPolicy;

    // File Persistence Configuration
    private int filePersistenceInstances;
    private boolean filePersistenceWorker;
    private int batchWriteIntervalMs;
    private int batchSize;
    private String dataDirectory;

    // Bloom Filter Configuration
    private int bloomFilterInstances;
    private boolean bloomFilterWorker;
    private int bloomFilterSize;
    private int numHashFunctions;

    // Request Router Configuration
    private int requestRouterInstances;
    private boolean requestRouterWorker;

    // Vert.x Options
    private int workerPoolSize;
    private int eventLoopPoolSize;
    private boolean clustered;

    public AppConfig(String configFilePath) throws IOException {
        String configContent = new String(Files.readAllBytes(Paths.get(configFilePath)));
        config = new JsonObject(configContent);

        // Load Cache Verticle Config
        JsonObject cacheConfig = config.getJsonObject("cacheVerticle");
        this.cacheVerticleInstances = cacheConfig.getInteger("instances");
        this.cacheSize = cacheConfig.getInteger("cacheSize");
        this.evictionPolicy = cacheConfig.getString("evictionPolicy");

        // Load File Persistence Verticle Config
        JsonObject filePersistenceConfig = config.getJsonObject("filePersistenceVerticle");
        this.filePersistenceInstances = filePersistenceConfig.getInteger("instances");
        this.filePersistenceWorker = filePersistenceConfig.getBoolean("worker");
        this.batchWriteIntervalMs = filePersistenceConfig.getInteger("batchWriteIntervalMs");
        this.batchSize = filePersistenceConfig.getInteger("batchSize");
        this.dataDirectory = filePersistenceConfig.getString("dataDirectory");

        // Load Bloom Filter Verticle Config
        JsonObject bloomFilterConfig = config.getJsonObject("bloomFilterVerticle");
        this.bloomFilterInstances = bloomFilterConfig.getInteger("instances");
        this.bloomFilterWorker = bloomFilterConfig.getBoolean("worker");
        this.bloomFilterSize = bloomFilterConfig.getInteger("bloomFilterSize");
        this.numHashFunctions = bloomFilterConfig.getInteger("numHashFunctions");

        // Load Request Router Verticle Config
        JsonObject requestRouterConfig = config.getJsonObject("requestRouterVerticle");
        this.requestRouterInstances = requestRouterConfig.getInteger("instances");

        // Load Vert.x Options Config
        JsonObject vertxConfig = config.getJsonObject("vertxOptions");
        this.workerPoolSize = vertxConfig.getInteger("workerPoolSize");
        this.eventLoopPoolSize = vertxConfig.getInteger("eventLoopPoolSize");
        this.clustered = vertxConfig.getBoolean("clustered");
    }

    public VertxOptions getVertxOptions() {
        return new VertxOptions()
                .setWorkerPoolSize(workerPoolSize)
                .setEventLoopPoolSize(eventLoopPoolSize);
    }
}