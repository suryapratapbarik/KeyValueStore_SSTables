To achieve thousands of reads and writes per second in a Key-Value Store that uses in-memory caching and periodically writes to a file for persistence, careful architectural and configuration decisions are crucial. Here’s a breakdown of the recommended verticle setup and configurations:

1. Verticle Setup

You need specialized verticles to handle specific tasks. A modular approach ensures scalability and efficient handling of tasks:

a. In-Memory Cache Verticle

	•	Purpose: Manages the in-memory cache for fast reads and writes.
	•	Tasks:
	•	Handles GET, PUT, and DELETE requests with low latency.
	•	Directly serves reads and writes from/to the cache.
	•	Type: Non-worker verticle (runs on the event loop).
	•	Instances: Number of CPU cores (or slightly higher).
	•	Each instance uses a thread from the event loop thread pool.

b. File Persistence Verticle

	•	Purpose: Periodically dumps new or modified data from the cache to disk (e.g., SSTable format).
	•	Tasks:
	•	Batches writes to minimize disk I/O overhead.
	•	Runs periodically or based on thresholds (e.g., every X seconds or when data size exceeds Y bytes).
	•	Type: Worker verticle (runs on a separate worker thread pool).
	•	Instances: 1–2 instances (as file I/O is inherently blocking).

c. Bloom Filter Update Verticle

	•	Purpose: Manages updates to the Bloom filter when new keys are added.
	•	Tasks:
	•	Receives key update events and updates the Bloom filter asynchronously.
	•	Type: Worker verticle (since updates involve computations).
	•	Instances: 1–2 instances (depends on the expected rate of key additions).

d. Request Router/Coordinator Verticle

	•	Purpose: Acts as a central point for routing requests to the appropriate verticles.
	•	Tasks:
	•	Routes GET/PUT/DELETE requests to the Cache Verticle.
	•	Routes periodic or bulk persistence tasks to the File Persistence Verticle.
	•	Type: Non-worker verticle.
	•	Instances: 1–2 instances (since this is a lightweight task).

2. Suggested Deployment

Let’s assume a system with 8 CPU cores. Here’s how you can configure the deployment:

Verticle Type	Instances	Threading Model	Purpose

In-Memory Cache Verticle	8 (1 per core)	Event Loop	Handles reads/writes with low latency.
File Persistence Verticle	1–2	Worker	Periodically writes data to disk.
Bloom Filter Update Verticle	1–2	Worker	Asynchronously updates the Bloom filter.
Request Router Verticle	1–2	Event Loop	Routes requests to other verticles.

3. Configuration Details

In-Memory Cache Verticle

	•	EventBus Address: cache.read, cache.write.
	•	Concurrency: Fully non-blocking; let Vert.x event loop handle parallelism.
	•	Cache Type: Use ConcurrentHashMap or other thread-safe in-memory cache implementations (e.g., Caffeine for LRU eviction).
	•	Optimization:
	•	Shard the cache across instances for better CPU utilization.

File Persistence Verticle

	•	EventBus Address: persistence.write.
	•	Batching: Write data to the file in batches (e.g., collect keys and values in memory for X seconds or until Y size).
	•	Disk Write Optimization:
	•	Use buffered I/O for efficient writes.
	•	Write asynchronously if possible.
	•	Trigger: Use a timer or listen to events for cache updates.

Bloom Filter Update Verticle

	•	EventBus Address: bloom.update.
	•	Bloom Filter Design:
	•	Optimize size and number of hash functions based on expected keys and false positive rate.
	•	Use lightweight hash functions (e.g., MurmurHash).
	•	Optimization:
	•	Batch updates for better performance (update multiple keys at once).

Request Router Verticle

	•	EventBus Address: N/A (entry point for clients).
	•	Load Balancing:
	•	Use Vert.x’s built-in round-robin or custom routing strategy to distribute requests across Cache Verticle instances.
	•	Interface:
	•	RESTful API or raw TCP for client communication.

4. Expected Performance

Reads

	•	Reads directly served from the In-Memory Cache have minimal latency.
	•	With sharded cache across 8 instances:
	•	Each instance handles a portion of the requests.
	•	Latency remains consistent as long as the Event Loop isn’t overloaded.

Writes

	•	Fast Path: Cache writes are quick, updating in-memory data.
	•	Background Path:
	•	Writes to disk are handled asynchronously, ensuring fast responses.
	•	Bloom filter updates are decoupled, so they don’t block the main path.

Throughput

	•	Reads: Thousands of reads per second (memory-bound).
	•	Writes: Near-memory speed for cache writes; disk writes are batched for efficiency.

Latency

	•	Reads: <1 ms (memory latency).
	•	Writes: <1 ms for cache writes; disk persistence latency depends on batch size and frequency.

5. Scaling Considerations

   •	Verticle Instances: Increase the number of Cache Verticles as CPU cores grow.
   •	EventBus Clustering:
   •	If scaling beyond a single machine, use a clustered Vert.x instance to distribute the workload.
   •	Cache Size:
   •	Monitor memory usage; consider eviction policies (e.g., LRU, LFU) to prevent memory overflow.

Summary

Deploy 8 Cache Verticles, 1–2 File Persistence Verticles, 1–2 Bloom Filter Update Verticles, and 1–2 Request Router Verticles. This configuration balances throughput and scalability while ensuring efficient reads, writes, and persistence. Fine-tune worker thread pool sizes and batch configurations for optimal disk I/O performance.