/**
 * @fileoverview Model data cache initialization
 * @module cache
 * @example
 * const newCache = require("ringo-pgclient/lib/cache");
 * 
 * module.exports = module.singleton("model-cache", () => newCache(1000));
 */

require("./utils").loadJars();

/**
 * Constructs a model data cache. This should only be called once and stored
 * in a singleton, as it's used across Client instances in different threads
 * @param {Number} size The size of the cache (defaults to 1000)
 * @returns {Cache} A new Cache instance
 * @function
 */
module.exports = function(size) {
    if (isNaN(parseInt(size, 10)) || !isFinite(size)) {
        size = 1000;
    }
    const builder = com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap.Builder();
    return builder.maximumWeightedCapacity(size).build();
};