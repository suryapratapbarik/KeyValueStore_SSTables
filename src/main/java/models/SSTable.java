package models;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

/**
 * SSTable implementation to manage key-value storage on disk.
 *
 * 	1.	File Structure:
 * 	•	The .sst file stores the actual key-value pairs in a newline-separated format.
 * 	•	The .index file stores key-to-offset mappings for efficient lookups.
 * 	2.	Write Operation:
 * 	•	Appends a key-value pair to the .sst file.
 * 	•	Updates the in-memory index and persists it to the .index file.
 * 	3.	Read Operation:
 * 	•	Uses the in-memory index to find the file offset for a key.
 * 	•	Reads the key-value pair from the .sst file at the calculated offset.
 * 	4.	Index Persistence:
 * 	•	The index is saved to the .index file for durability.
 * 	•	Loaded into memory on SSTable initialization.
 * 	5.	Bloom Filter Integration:
 * 	•	The mightContain method is included for Bloom filter-based probabilistic checks before searching the SSTable.
 * 	6.	Thread Safety:
 * 	•	The write, read, and delete methods are synchronized to ensure thread safety for concurrent access.
 *
 * 	Customizations and Considerations
 *
 * 	1.	Size of the BitSet:
 * 	•	The size of the BitSet (1000 in the example) should be chosen based on the expected number of elements in the filter and the acceptable false positive rate.
 * 	2.	Number of Hash Functions:
 * 	•	The number of hash functions (numHashFunctions) should be chosen to minimize the false positive rate. A higher number of hash functions generally reduces the false positive rate but requires more computations per add or mightContain call.
 * 	3.	False Positives:
 * 	•	A Bloom Filter may return false positives. This means that it may tell you a key is present even if it’s not, but it will never tell you a key is not present if it actually is.
 * 	4.	Space Efficiency:
 * 	•	Bloom Filters are memory efficient for set membership tests, especially when the size of the set is large. However, their accuracy depends on the number of hash functions and the size of the bit set.
 *
 */
public class SSTable {
    private final Path sstableFilePath;
    private final Path indexFilePath;
    private final Map<String, Long> index; // Key-to-file offset mapping for quick lookups
    private final BloomFilter bloomFilter;
    private final String tableName;

    public SSTable(String directory, String tableName, int bloomFilterSize, int bloomHashCount) throws IOException {
        // Ensure the directory exists
        Files.createDirectories(Paths.get(directory));
        this.tableName = tableName;
        this.sstableFilePath = Paths.get(directory, tableName + ".sst");
        this.indexFilePath = Paths.get(directory, tableName + ".index");
        this.index = new HashMap<>();
        this.bloomFilter = new BloomFilter(bloomFilterSize, bloomHashCount);

        // Load index from disk if it exists
        if (Files.exists(indexFilePath)) {
            loadIndex();
        }

    }

    /**
     * Adds a key-value pair to the SSTable.
     */
    public synchronized void write(String key, String value) throws IOException {
        try (RandomAccessFile sstableFile = new RandomAccessFile(sstableFilePath.toFile(), "rw")) {
            // Move to the end of the file
            sstableFile.seek(sstableFile.length());

            // Write key-value pair
            long offset = sstableFile.getFilePointer();
            String entry = key + "," + value + "\n";
            sstableFile.write(entry.getBytes());

            // Update the in-memory index
            index.put(key, offset);


            bloomFilter.add(key);

            // Persist the index
            saveIndex();
        }
    }

    /**
     * Retrieves a value for a given key from the SSTable.
     */
    public synchronized Optional<String> read(String key) throws IOException {

        if (!bloomFilter.mightContain(key)) {
            return Optional.empty(); // Skip reading if Bloom filter suggests key is absent
        }

        Long offset = index.get(key);
        if (offset == null) {
            return Optional.empty(); // Key not found in this SSTable
        }

        try (RandomAccessFile sstableFile = new RandomAccessFile(sstableFilePath.toFile(), "r")) {
            // Seek to the file offset
            sstableFile.seek(offset);

            // Read the line
            String line = sstableFile.readLine();
            String[] parts = line.split(",", 2);

            if (parts.length == 2 && parts[0].equals(key)) {
                return Optional.of(parts[1]); // Return the value
            }
        }

        return Optional.empty();
    }

    /**
     * Loads the index file into memory.
     */
    private void loadIndex() throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(indexFilePath)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 2) {
                    String key = parts[0];
                    long offset = Long.parseLong(parts[1]);
                    index.put(key, offset);

                    // Add the key to the Bloom filter during index load
                    bloomFilter.add(key);
                }
            }
        }
    }

    /**
     * Saves the in-memory index to the index file on disk.
     */
    private void saveIndex() throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(indexFilePath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            for (Map.Entry<String, Long> entry : index.entrySet()) {
                writer.write(entry.getKey() + "," + entry.getValue());
                writer.newLine();
            }
        }
    }

    /**
     * Checks if the SSTable might contain the given key using an external Bloom filter.
     * (Assuming a Bloom filter instance is provided elsewhere in the system.)
     */
    public boolean mightContain(String key) {
        return bloomFilter.mightContain(key);
    }

    /**
     * Returns the total number of keys in the SSTable.
     */
    public int getKeyCount() {
        return index.size();
    }

    /**
     * Deletes the SSTable files (data and index).
     */
    public synchronized void delete() throws IOException {
        Files.deleteIfExists(sstableFilePath);
        Files.deleteIfExists(indexFilePath);
    }
    

    /**
     * Gets the Bloom Filter for the SSTable (for testing purposes).
     */
    public BloomFilter getBloomFilter() {
        return bloomFilter;
    }

    public static void main(String[] args) throws IOException {
        // Create an SSTable with a Bloom filter size of 1000 and 3 hash functions
        SSTable sstable = new SSTable("data", "example", 1000, 3);

        // Write some keys to the SSTable
        sstable.write("key1", "value1");
        sstable.write("key2", "value2");
        sstable.write("key3", "value3");

        // Read from the SSTable
        System.out.println("Read key1: " + sstable.read("key1").orElse("Not Found"));
        System.out.println("Read key2: " + sstable.read("key2").orElse("Not Found"));
        System.out.println("Read key3: " + sstable.read("key3").orElse("Not Found"));

        // Bloom filter check
        BloomFilter bloomFilter = sstable.getBloomFilter();
        System.out.println("Bloom filter mightContain 'key2': " + bloomFilter.mightContain("key2"));
        System.out.println("Bloom filter mightContain 'key4': " + bloomFilter.mightContain("key4"));

        // Delete the SSTable
        sstable.delete();
    }

    public List<String> getAllKeys() {
        return new ArrayList<>(index.keySet());
    }

    public long getCreationTime() throws IOException {
        return Files.getLastModifiedTime(sstableFilePath).toMillis();
    }

    public String getTableName() {
        return this.tableName;
    }


}