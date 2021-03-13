/**
 * @module cache
 */

addToClasspath(module.resolve("../jars/concurrentlinkedhashmap-lru-1.4.2.jar"));

/**
 * Constructs a model data cache. This should only be called once and stored
 * in a singleton, as it's used across Client instances
 * @param {Number} size The size of the cache (defaults to 1000)
 * @returns {Cache} A new Cache instance
 * @type Cache
 * @constructor
 */
module.exports = function(size) {
    if (isNaN(parseInt(size, 10)) || !isFinite(size)) {
        size = 1000;
    }
    const builder = com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap.Builder();
    return builder.maximumWeightedCapacity(size).build();
};