/*
 *
 * extendible_hash.h : implementation of in-memory hash table using extendible
 * hashing
 *
 * Functionality: The buffer pool manager must maintain a page table to be able
 * to quickly map a PageId to its corresponding memory location; or alternately
 * report that the PageId does not match any currently-buffered page.
 */

#pragma once

#include <cstdlib>
#include <vector>
#include <string>
#include <map>
#include "hash/hash_table.h"
#include <mutex>
#include <memory>
#include <cstdio>
namespace cmudb {
#define GLOBAL_HASH_TABLE \
    std::vector<std::shared_ptr<Bucket>>

template <typename K, typename V>
class ExtendibleHash : public HashTable<K, V> {
public:
  // constructor
  ExtendibleHash(size_t size);
  // helper function to generate hash addressing
  size_t HashKey(const K &key);
  // helper function to get global & local depth
  int GetGlobalDepth() const;
  int GetLocalDepth(int bucket_id) const;
  int GetNumBuckets() const;
  // lookup and modifier
  bool Find(const K &key, V &value) override;
  bool Remove(const K &key) override;
  void Insert(const K &key, const V &value) override;
  void PrintCurrentState();
private:
  // add your own member variables here
  class Bucket
  {
    public:
        Bucket() = default;
        Bucket(size_t size) :buck_num(0), buck_max_num(size), depth(0),bucket_val(0) {}
        bool buck_find(const K &key, V& value) {
            auto buck_item = buck_group.find(key);
            if (buck_item == buck_group.end())
                return false;
            else {
                value = buck_item->second;
                return true;
            }
         } 
        /* return current bucket size */
        size_t buck_insert(const K& key, const V &value) {
            buck_group.insert(std::make_pair(key, value));
            buck_num++;
            return buck_num;
        }
        typename std::map<K,V>::iterator buck_remove(const K &key) {
            auto buck_item = buck_group.find(key);
            /* key do not exist */
            if (buck_item == buck_group.end())
                return buck_group.end();
            auto iter = buck_group.erase(buck_item);
            buck_num--;
            return iter;
        }
        void buck_clear() {
            buck_group.clear();
        }
        size_t buck_num;
        size_t buck_max_num;
        size_t depth;
        size_t bucket_val;
        std::map<K, V> buck_group;
  };
  std::unique_ptr<Bucket> split(std::shared_ptr<Bucket> origin);
  mutable std::mutex global_mut; 
  size_t max_bucket_size; // max size for each bucket
  size_t global_depth; //  current global depth
  size_t bucket_count; // how many bucket now
  GLOBAL_HASH_TABLE global_hash_table;
};
} // namespace cmudb
