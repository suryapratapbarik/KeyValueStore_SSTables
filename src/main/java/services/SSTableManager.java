package services;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import models.SSTable;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SSTableManager {

    private final String directory;
    private final int bloomFilterSize;
    private final int bloomHashCount;
    private final int maxKeysPerSSTable;

    private final List<SSTable> sstables;
    private SSTable activeSSTable;

    private final AtomicInteger sstableCounter = new AtomicInteger(0);

    private final Vertx vertx;

    public SSTableManager(String directory, int bloomFilterSize, int bloomHashCount, int maxKeysPerSSTable, Vertx vertx) throws IOException {
        this.vertx = vertx;
        this.directory = directory;
        this.bloomFilterSize = bloomFilterSize;
        this.bloomHashCount = bloomHashCount;
        this.maxKeysPerSSTable = maxKeysPerSSTable;

        Files.createDirectories(Paths.get(directory));
        this.sstables = new ArrayList<>();
        registerEventBusHandlers();
        loadExistingSSTables();
        initializeActiveSSTable();
    }

    private void registerEventBusHandlers() {
        // Handle PUT requests to write key-value pairs
        vertx.eventBus().consumer("sstable.compact", this::handleCompactTablesRequest);

    }

    private void handleCompactTablesRequest(Message<Object> message) {

        try {
            finalizeActiveSSTable();
            initializeActiveSSTable();
            // Reply with the value or NOT_FOUND
            message.reply("Compaction successful");
        } catch (Exception e) {
            System.err.println("Error during Compaction: " + e.getMessage());
            message.fail(500, e.getMessage());
        }


    }

    private void loadExistingSSTables() throws IOException {
        // Load existing SSTables from disk
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(directory), "*.sst")) {
            for (Path path : stream) {
                String fileName = path.getFileName().toString();
                String tableName = fileName.substring(0, fileName.lastIndexOf('.'));
                SSTable sstable = new SSTable(directory, tableName, bloomFilterSize, bloomHashCount);
                sstables.add(sstable);
            }
        }
        sstables.sort(Comparator.comparing(ssTable -> {
            try {
                return ssTable.getCreationTime();
            } catch (IOException e) {
                System.out.println(e.getMessage());
                throw new RuntimeException(e);
            }
        }));
    }

    private void initializeActiveSSTable() throws IOException {
        String newTableName = "sstable_" + sstableCounter.incrementAndGet();
        activeSSTable = new SSTable(directory, newTableName, bloomFilterSize, bloomHashCount);
        sstables.add(activeSSTable);
    }

    public synchronized void put(String key, String value) throws IOException {
        vertx.executeBlocking(promise -> {
            try {
                activeSSTable.write(key, value);
                promise.complete();
            } catch (IOException e) {
                System.out.println("Error writing to SSTable: " + e.getMessage());
                promise.fail(e);
            }
        }, asyncResult -> {
            if (asyncResult.succeeded()) {
                System.out.println("File write completed successfully.");
            } else {
                System.out.println("File write failed: " + asyncResult.cause().getMessage());
            }
        });

        if (activeSSTable.getKeyCount() >= maxKeysPerSSTable) {
            vertx.eventBus().send("sstable.compact", key);
        }
    }

    public synchronized Optional<String> get(String key) throws IOException {
        for (SSTable sstable : sstables) {
            if (sstable.mightContain(key)) {
                Optional<String> value = sstable.read(key);
                if (value.isPresent()) {
                    return value;
                }
            }
        }
        return Optional.empty();
    }

    private void finalizeActiveSSTable() throws IOException {
        System.out.println("Finalizing active SSTable: " + activeSSTable.getTableName());
        //activeSSTable.close();
        triggerCompactionIfNeeded();
    }

    private void triggerCompactionIfNeeded() throws IOException {
        if (sstables.size() > 3) {
            System.out.println("Triggering SSTable compaction...");
            compactSSTables();
        }
    }

    private void compactSSTables() {
        List<SSTable> toCompact = new ArrayList<>(sstables.subList(0, 3));
        String compactedTableName = "sstable_" + sstableCounter.incrementAndGet();
        SSTable compactedSSTable;
        try {
            compactedSSTable = new SSTable(directory, compactedTableName, bloomFilterSize, bloomHashCount);
        } catch (IOException e) {
            System.out.println("compactSSTables failed: " + e.getMessage());
            throw new RuntimeException(e);
        }

        for (SSTable sstable : toCompact) {
            for (String key : sstable.getAllKeys()) {
                Optional<String> value = null;
                try {
                    value = sstable.read(key);
                } catch (IOException e) {
                    System.out.println("sstable.read(key) failed: " + e.getMessage());
                    throw new RuntimeException(e);
                }
                SSTable finalCompactedSSTable = compactedSSTable;
                value.ifPresent(v -> {
                    try {
                        finalCompactedSSTable.write(key, v);
                    } catch (IOException e) {
                        System.out.println("finalCompactedSSTable.write( failed: " + e.getMessage());
                        e.printStackTrace();
                    }
                });
            }
            try {
                sstable.delete();
            } catch (IOException e) {
                System.out.println("sstable.delete failed: " + e.getMessage());
                throw new RuntimeException(e);
            }
        }

        sstables.removeAll(toCompact);
        sstables.add(compactedSSTable);
    }
}