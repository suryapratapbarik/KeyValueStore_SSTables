package verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;

import java.util.stream.Collectors;

/**
 * The Request Router Verticle should not be a worker verticle. It is designed to handle lightweight routing tasks, forwarding requests to other verticles like the Cache or File Persistence Verticle.
 * <p>
 * Here’s why it should not be a worker:
 * <p>
 * Reasons to Keep Request Router Verticle Non-Worker
 * <p>
 * 1.	Low Latency:
 * •	A non-worker verticle (event-loop based) operates with low latency, as it processes tasks on the Vert.x event loop. This is ideal for routing requests since the routing itself does not involve blocking operations.
 * 2.	Threading Model:
 * •	A worker verticle uses a separate thread pool, introducing context switching and queuing delays. For routing tasks, this overhead is unnecessary and would degrade performance.
 * 3.	Non-Blocking Nature:
 * •	Routing is usually a simple, non-blocking task that doesn’t require the multithreaded capabilities of a worker verticle. It’s better suited for the event-loop model.
 * 4.	Concurrency and Throughput:
 * •	The event loop model ensures high concurrency and throughput for lightweight operations. This matches the use case of a router forwarding requests to the appropriate verticle (cache, persistence, etc.).
 * <p>
 * When to Consider Worker for Request Router
 * <p>
 * You would consider a worker verticle only if the router needs to:
 * •	Perform CPU-intensive operations (e.g., complex request transformation or computation before forwarding).
 * •	Execute blocking tasks, like interacting with an external system or database directly (though in such cases, it’s better to offload that to another worker verticle).
 * <p>
 * Yes, a Vert.x application can run multiple instances of a RequestRouterVerticle (or any verticle) even if it is not clustered. Vert.x provides built-in support for deploying multiple instances of a verticle within a single JVM process. This feature is useful for improving scalability and performance on multicore systems.
 * <p>
 * How Multiple Verticle Instances Work in Vert.x
 * <p>
 * •	When you deploy a verticle, you can specify the number of instances to deploy using the instances option in DeploymentOptions.
 * •	Each instance of the verticle runs in its own context but shares the same Vert.x instance, meaning they can communicate via the event bus or shared resources (e.g., shared data structures).
 * •	Even without clustering, Vert.x can distribute incoming requests or tasks to multiple instances of the same verticle to utilize CPU cores efficiently.
 * <p>
 * Deploying Multiple Instances of RequestRouterVerticle
 * In this example:
 * •	Multiple instances of RequestRouterVerticle will be deployed.
 * •	These instances can run concurrently on separate threads, taking advantage of multicore processors.
 * <p>
 * Behavior in a Non-Clustered Environment
 * <p>
 * •	Instance Isolation: Each instance has its own lifecycle and configuration but shares the same Vert.x instance.
 * •	Shared Event Bus: The instances can communicate via the event bus, even though the application is not clustered.
 * •	Concurrency: Vert.x distributes events across the verticle instances, allowing concurrent handling of requests or events.
 * <p>
 * Benefits of Multiple Instances
 * <p>
 * 1.	Improved CPU Utilization:
 * •	By deploying multiple instances, Vert.x can spread the load across multiple event loop threads. This is particularly useful for multicore CPUs, where each thread can execute an instance.
 * 2.	Higher Throughput:
 * •	With more verticle instances, the application can handle more concurrent requests or events, as each instance processes events independently.
 * 3.	Non-Blocking Model:
 * •	Even with multiple instances, Vert.x ensures that each instance adheres to its non-blocking, event-driven architecture.
 * <p>
 * Considerations
 * <p>
 * 1.	Thread Safety:
 * •	If the verticle accesses shared resources (e.g., a shared map or database connection), ensure proper synchronization or use Vert.x mechanisms like SharedData or the event bus.
 * 2.	Avoid Blocking Code:
 * •	Each verticle instance runs on an event loop thread, so avoid blocking operations in the verticle to maintain responsiveness.
 * 3.	Scaling:
 * •	While multiple instances improve parallelism within a single JVM, they don’t provide the distributed scaling benefits of clustering. To scale across multiple JVMs or machines,
 *      you would need to enable Vert.x clustering.
 * <p>
 * Summary
 * <p>
 * A Vert.x application can indeed run multiple instances of RequestRouterVerticle in a non-clustered setup. This approach leverages multicore CPUs to improve concurrency and throughput.
 * While it doesn’t provide distributed scaling, it’s a great way to scale applications within a single JVM process. Use it effectively to balance the load and maximize hardware utilization.
 */
public class RequestRouterVerticle extends AbstractVerticle {

    @Override
    public void start(Promise<Void> promise) {
        try {
            Router router = Router.router(vertx);
            router.route().handler(BodyHandler.create());

            router.post("/api/put").handler(this::handlePut);
            router.get("/api/get").handler(this::handleGet);

            vertx.createHttpServer().requestHandler(router).listen(8080);
            System.out.println("KV Store running on port 8080");
            promise.complete();
            System.out.println("RequestRouterVerticle started successfully.");
        } catch (Exception e) {
            promise.fail(e);
            System.err.println("Failed to start RequestRouterVerticle: " + e.getMessage());
        }

    }

    private void handlePut(io.vertx.ext.web.RoutingContext context) {
        var body = context.getBodyAsJson();
        var data = body.getJsonArray("newKeys").stream()
                .map(obj -> (io.vertx.core.json.JsonObject) obj)
                .collect(Collectors.toMap(
                        json -> json.getString("key"),
                        json -> String.join(",", json.getJsonArray("value").getList())
                ));

        // Send write request to cache and return response;
        vertx.eventBus().request("cache.write", data, ar -> {
            if (ar.succeeded()) {
                context.response().setStatusCode(200).end("Keys added successfully to in-memory cache.");

            } else {
                context.response().setStatusCode(500).end("Error during PUT: " + ar.cause().getMessage());
            }
        });
        vertx.eventBus().request("sstable.write", data, ar -> {
            if (ar.succeeded()) {
                //System.out.println("Keys added successfully.");
            } else {
                //System.out.println("Error during PUT: " + ar.cause().getMessage());
            }
        });

    }

    private void handleGet(io.vertx.ext.web.RoutingContext context) {
        // Get the request body as a JSON object
        var body = context.getBodyAsJson();

        JsonArray keys = body.getJsonArray("keys");
        JsonArray values = new JsonArray();
        if (keys != null) {
            for (Object key : keys) {
                vertx.eventBus().request("cache.read", key, ar -> {
                    if (ar.succeeded()) {
                        String value = (String) ar.result().body();
                        if ("NOT_FOUND".equals(value)) {
                            System.out.println("Key not found in cache.");

                            // Send read request to FilePersistenceVerticle
                            vertx.eventBus().request("sstable.read", key, ar2 -> {
                                if (ar2.succeeded()) {
                                    String value2 = (String) ar2.result().body();
                                    if ("NOT_FOUND".equals(value2)) {
                                        values.add(new JsonArray());
                                        System.out.println("Key not found in File.");
                                    } else {
                                        values.add(new JsonArray(value));
                                    }
                                } else {
                                    context.response().setStatusCode(500).end("Error during GET from File: " + ar.cause().getMessage());
                                }
                            });
                        } else {
                            JsonArray jsonArray = ((value != null) ? new JsonArray(value) : new JsonArray());
                            values.add(jsonArray);
                        }
                    } else {
                        context.response().setStatusCode(500).end("Error during GET from cache: " + ar.cause().getMessage());
                    }
                });
            }
        }
        context.response()
                .setStatusCode(200)
                .putHeader("content-type", "application/json")
                .end(new JsonObject().put("value", values).encode());
    }
}