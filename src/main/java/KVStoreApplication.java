import config.AppConfig;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import verticles.BloomFilterUpdateVerticle;
import verticles.CacheVerticle;
import verticles.RequestRouterVerticle;
import verticles.FilePersistenceVerticle;

public class KVStoreApplication {

    public static void main(String[] args) {
        try {
            // Load configuration
            AppConfig config = new AppConfig("config.json");

            // Create Vert.x instance with configured options
            Vertx vertx = Vertx.vertx(config.getVertxOptions());

            /* Simple vertx.deployVerticle() implementations:

            Future<String> deployVerticle(String name, DeploymentOptions options)
            Future<String> deployVerticle(Verticle verticle, DeploymentOptions options)
            void deployVerticle(Verticle verticle, Handler<AsyncResult<String>> completionHandler)
            void deployVerticle(String name, DeploymentOptions options, Handler<AsyncResult<String>> completionHandler)
            */

            // Deploy Cache Verticle
            vertx.deployVerticle(CacheVerticle.class.getName(),
                    new DeploymentOptions()
                            .setInstances(config.getCacheVerticleInstances())
                            .setWorker(false)); // Event-loop model

            // Deploy File Persistence Verticle
            vertx.deployVerticle(FilePersistenceVerticle.class.getName(),
                    new DeploymentOptions()
                            .setInstances(config.getFilePersistenceInstances())
                            .setWorker(config.isFilePersistenceWorker())); // Worker model

            // Deploy Bloom Filter Update Verticle
            vertx.deployVerticle(BloomFilterUpdateVerticle.class.getName(),
                    new DeploymentOptions()
                            .setInstances(config.getBloomFilterInstances())
                            .setWorker(config.isBloomFilterWorker())); // Worker model

            // Deploy Request Router Verticle
            /*
The Request Router Verticle should not be a worker verticle. It is designed to handle lightweight routing tasks, forwarding requests to other verticles like the Cache or File Persistence Verticle.

Here’s why it should not be a worker:

Reasons to Keep Request Router Verticle Non-Worker

	1.	Low Latency:
	•	A non-worker verticle (event-loop based) operates with low latency, as it processes tasks on the Vert.x event loop. This is ideal for routing requests since the routing itself does not involve blocking operations.
	2.	Threading Model:
	•	A worker verticle uses a separate thread pool, introducing context switching and queuing delays. For routing tasks, this overhead is unnecessary and would degrade performance.
	3.	Non-Blocking Nature:
	•	Routing is usually a simple, non-blocking task that doesn’t require the multithreaded capabilities of a worker verticle. It’s better suited for the event-loop model.
	4.	Concurrency and Throughput:
	•	The event loop model ensures high concurrency and throughput for lightweight operations. This matches the use case of a router forwarding requests to the appropriate verticle (cache, persistence, etc.).

When to Consider Worker for Request Router

You would consider a worker verticle only if the router needs to:
	•	Perform CPU-intensive operations (e.g., complex request transformation or computation before forwarding).
	•	Execute blocking tasks, like interacting with an external system or database directly (though in such cases, it’s better to offload that to another worker verticle).
             */
            vertx.deployVerticle(RequestRouterVerticle.class.getName(),
                    new DeploymentOptions()
                            .setInstances(config.getRequestRouterInstances())
                            .setWorker(false)); // Event-loop model


            System.out.println("KV Store Application started successfully!");

            // OPTIONAL
            // Add shutdown hook to gracefully stop Vert.x when the application is terminated
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutting down KV Store application...");
                vertx.close(ar -> {
                    if (ar.succeeded()) {
                        System.out.println("Application shut down cleanly.");
                    } else {
                        System.err.println("Error during shutdown: " + ar.cause().getMessage());
                    }
                });
            }));

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Failed to start the KV Store Application: " + e.getMessage());
        }
    }
}