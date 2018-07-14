#include <list>
#include <mutex>
#include <bitset>
#include "hash/extendible_hash.h"
#include "page/page.h"

namespace cmudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size) : max_bucket_size(size),
global_depth(0), bucket_count(0)
{
    global_hash_table.emplace_back(new Bucket(size));
    bucket_count++;
}


/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) {
  return std::hash<K>{ }(key);
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
    std::lock_guard<std::mutex> guard(global_mut);
    return global_depth;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
    std::lock_guard<std::mutex> guard(global_mut);
    int hash_table_size = global_hash_table.size();
    if (bucket_id < hash_table_size)
        return global_hash_table[bucket_id]->depth;
    else
        return -1;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
    std::lock_guard<std::mutex> guard(global_mut);
  return bucket_count;
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
    std::lock_guard<std::mutex> guard(global_mut);
    size_t pos = HashKey(key) & ((1 <<global_depth) - 1);
    if (global_hash_table[pos]) {
        auto tmp_buck = global_hash_table[pos];
        if (tmp_buck->buck_group.find(key) != tmp_buck->buck_group.end()) {
            value = tmp_buck->buck_group[key];
            return true;
        }
    }
    return false;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
  std::lock_guard<std::mutex> guard(global_mut);
  size_t cnt = 0;
  size_t pos = HashKey(key) & ((1 << global_depth) - 1);
  if (global_hash_table[pos]) {
      auto bucket = global_hash_table[pos];
      cnt += bucket->buck_group.erase(key);
      bucket->buck_num--;
  }
  return cnt != 0;
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
    std::lock_guard<std::mutex> guard(global_mut);
    size_t pos = HashKey(key) & ((1 << global_depth) - 1);
    std::shared_ptr<Bucket> tmp = global_hash_table[pos];
    if (tmp == nullptr) {
        tmp = std::make_shared<Bucket>(max_bucket_size);
        tmp->depth = global_depth;
        tmp->bucket_val = pos;
    }
    /* check whether the bucket already contains the item */
    V tmpvalue = value;
    if (tmp->buck_find(key, tmpvalue)) 
        return;
    size_t buck_num = tmp->buck_insert(key, value);
    size_t old_val = tmp->bucket_val;
    size_t old_depth = tmp->depth;
    if (buck_num > max_bucket_size) {
        /* split original bucket */
        std::shared_ptr<Bucket> new_buck = split(tmp);
        if (tmp->depth > global_depth) {
            /* redistribute */
            size_t glo_hash_table_size = global_hash_table.size();
            size_t load_factor = (1 << (tmp->depth - global_depth));
            global_depth = tmp->depth;
            global_hash_table.resize(glo_hash_table_size * load_factor);
            global_hash_table[tmp->bucket_val] = tmp;
            global_hash_table[new_buck->bucket_val] = new_buck;
            for (size_t i = 0; i < glo_hash_table_size; i++) {
                if (global_hash_table[i] != nullptr) {
                    if ((i < global_hash_table[i]->bucket_val) ||
                ((i & ((1 << global_hash_table[i]->depth) - 1)) != global_hash_table[i]->bucket_val))
                        global_hash_table[i].reset();
                    else {
                        auto step = 1 << global_hash_table[i]->depth;
                        for (size_t j = i + step; j < global_hash_table.size(); j += step)
                            global_hash_table[j] = global_hash_table[i];
                         }
                }
            } 
        } else {
            for (size_t i = old_val; i < global_hash_table.size(); i += 1 << old_depth)
                global_hash_table[i].reset();
            global_hash_table[tmp->bucket_val] = tmp;
            global_hash_table[new_buck->bucket_val] = new_buck;
            for (size_t i = tmp->bucket_val; i < global_hash_table.size(); i += 1 << tmp->depth)
                global_hash_table[i] = global_hash_table[tmp->bucket_val];
            for (size_t i = new_buck->bucket_val; i < global_hash_table.size(); i += 1 << new_buck->depth)
                global_hash_table[i] = global_hash_table[new_buck->bucket_val];
        }
    }
}
template <typename K, typename V>
std::unique_ptr<typename ExtendibleHash<K,V>::Bucket> ExtendibleHash<K,V>::split(std::shared_ptr<Bucket> origin) {
    std::unique_ptr<Bucket> ret(new Bucket(origin->buck_max_num));
    ret->depth = origin->depth;
    while (ret->buck_group.empty()) {
        ret->depth++;
        origin->depth++;
        for (auto item = origin->buck_group.begin(); item != origin->buck_group.end();) {
            /* depth = 3 ; since it increases before, ret->depth -1 still be 3
             * 0001 -> 1000 (the fourth position 
             */
            if (HashKey(item->first) & (1 << (ret->depth - 1))) {
                ret->buck_insert(item->first, item->second);
                ret->bucket_val = HashKey(item->first) & ((1 << ret->depth) - 1);
                item = origin->buck_remove(item->first);
            } else
                item++;
            if (origin->buck_group.empty()){
                origin->buck_group.swap(ret->buck_group);
                origin->bucket_val = ret->bucket_val;
            }
        }
    }
    bucket_count++;
    return ret;
}

template <typename K, typename V>
void ExtendibleHash<K,V>::PrintCurrentState() {
    std::printf("Current list state: \n");
    int buck_counter = 0;
    for (auto iter = global_hash_table.begin(); 
            iter != global_hash_table.end();
            iter++) {
        if (*iter == nullptr) {
            //printf("Bucket num %d has 0 element: \n", buck_counter);
            buck_counter++;
            continue;
        }
        else 
            printf("Bucket num %d has %zo element: \n",buck_counter, (*iter)->buck_num);
        for (auto iter2 = (*iter)->buck_group.begin();
                iter2 != (*iter)->buck_group.end();
                iter2++) {
            printf("%zo", HashKey(iter2->first));
            printf(" ");
        }
        printf("\n");
        buck_counter++;
    }
    printf("Total bucket number is %zo \n", bucket_count);
}


template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
