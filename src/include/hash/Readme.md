## Hash Table

### Description
This section implements an extensible hash table. Extensible hash table is used inside `BufferPoolManager` to help track current pages stored in memory. `ExtensibleHash` is a vector of `Bucket`, each `Bucket` stores a fixed amount of `key-value` pairs. For users, if you need to find a page, you need to search the `ExtensibleHash` for a give `page_id`, and this will return you a `Page*`. As a result, `key-value` pairs are actually `page_id-Page*` pairs. 

#### Data Struct
`ExtensibleHash` components:
```
mutex latch_ //protect access to hash table, each acess will require the latch
size_t bucket_size_ // the largest number of elements in a bucket
int bucket_num_ // bucket number
int depth // 
size_t pair_num_ // number of key-value pairs
vector<shared_ptr<Bucket>> directory_ // bucket vector
```
```
struct Bucket {
    ...
    map<K,V> items // key-value pairs
    size_t id // bucket id
    int depth
}
```

#### Operations
##### Find
1. use hash function to get bucket index
2. use key to index in the bucket item

##### Insert
