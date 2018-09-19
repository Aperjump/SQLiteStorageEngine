## Hash Table

### Description
This section implements an extensible hash table. Extensible hash table is used inside `BufferPoolManager` to help track current pages stored in memory. `ExtensibleHash` is a vector of `Bucket`, each `Bucket` stores a fixed amount of `key-value` pairs. For users, if you need to find a page, you need to search the `ExtensibleHash` for a give `page_id`, and this will return you a `Page*`. As a result, `key-value` pairs are actually `page_id-Page*` pairs. 

