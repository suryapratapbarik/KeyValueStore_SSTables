package models;

import java.util.BitSet;
import java.util.Random;

/**
 * Explanation
 *
 * 	1.	BitSet:
 * 	•	The core of the Bloom Filter is a BitSet, which is a sequence of bits that we manipulate to represent whether certain positions have been “set” (i.e., associated with a key).
 * 	2.	Hash Functions:
 * 	•	The class uses multiple hash functions (numHashFunctions), which are applied to the input key. Each hash function produces an index in the bit set, where the corresponding bit is set to true to indicate that the key is present.
 * 	•	Each hash function is generated using the key’s hashCode, with an additional increment (i) to diversify the hash values.
 * 	3.	Add Method:
 * 	•	The add() method applies all the hash functions to the key and sets the corresponding bits in the BitSet.
 * 	4.	MightContain Method:
 * 	•	The mightContain() method checks whether a key might be present in the Bloom Filter. It applies all the hash functions to the key and checks whether the corresponding bits are all true. If any bit is false, the key is definitely not in the filter.
 * 	5.	BitSet Size and Hash Function Count:
 * 	•	The bitSetSize defines the size of the BitSet, and numHashFunctions determines the number of different hash functions used for checking the membership.
 * 	6.	Clear Method:
 * 	•	The clear() method resets the BitSet, clearing all bits to false, essentially clearing the Bloom Filter.
 */
public class BloomFilter {
    private final BitSet bitSet;
    private final int bitSetSize;
    private final int numHashFunctions;

    // Constructor to initialize Bloom Filter with a specified bit set size and number of hash functions
    public BloomFilter(int bitSetSize, int numHashFunctions) {
        this.bitSetSize = bitSetSize;
        this.numHashFunctions = numHashFunctions;
        this.bitSet = new BitSet(bitSetSize);
    }

    // Hash function to calculate an index in the bitSet based on the key
    private int[] getHashes(String key) {
        int[] hashes = new int[numHashFunctions];
        Random random = new Random(key.hashCode());
        for (int i = 0; i < numHashFunctions; i++) {
            hashes[i] = Math.abs(random.nextInt() % bitSetSize) ;
        }
        return hashes;
    }

    // Adds a key to the Bloom Filter by setting bits at hash positions
    public void add(String key) {
        int[] hashValues = getHashes(key);
        for (int hashIndex : hashValues) {
            bitSet.set(hashIndex);
        }
    }

    // Checks if a key is possibly in the Bloom Filter (false positives are possible)
    public boolean mightContain(String key) {
        int[] hashValues = getHashes(key);
        for (int hashIndex : hashValues) {
            if (!bitSet.get(hashIndex)) {
                return false; // Key definitely not in the filter
            }
        }
        return true; // Key might be in the filter (possible false positive)
    }

    // Gets the bit set size (for debugging or analysis purposes)
    public int getBitSetSize() {
        return bitSetSize;
    }

    // Gets the number of hash functions (for debugging or analysis purposes)
    public int getNumHashFunctions() {
        return numHashFunctions;
    }

    // Clears the Bloom Filter (resets the bit set)
    public void clear() {
        bitSet.clear();
    }

    public static void main(String[] args) {
        // Create a Bloom Filter with a bit set size of 1000 and 3 hash functions
        BloomFilter bloomFilter = new BloomFilter(1000, 3);

        // Add keys to the Bloom Filter
        bloomFilter.add("apple");
        bloomFilter.add("banana");
        bloomFilter.add("cherry");

        // Check if keys are in the Bloom Filter
        System.out.println("Might contain 'apple': " + bloomFilter.mightContain("apple"));
        System.out.println("Might contain 'banana': " + bloomFilter.mightContain("banana"));
        System.out.println("Might contain 'grape': " + bloomFilter.mightContain("grape")); // False positive possible
    }
}