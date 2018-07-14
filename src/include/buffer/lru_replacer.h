/**
 * lru_replacer.h
 *
 * Functionality: The buffer pool manager must maintain a LRU list to collect
 * all the pages that are unpinned and ready to be swapped. The simplest way to
 * implement LRU is a FIFO queue, but remember to dequeue or enqueue pages when
 * a page changes from unpinned to pinned, or vice-versa.
 */

#pragma once

#include "buffer/replacer.h"
#include "hash/extendible_hash.h"
#include <mutex>
#include <list>
#include <unordered_map>

namespace cmudb {

template <typename T> class LRUReplacer : public Replacer<T> {
public:
  // T will be Page*
  // do not change public interface
  LRUReplacer();

  ~LRUReplacer();

  void Insert(const T &value);

  bool Victim(T &value);

  bool Erase(const T &value);

  size_t Size();

private:
  // add your member variables here
  std::mutex mut;
  std::list<T> val;
  std::unordered_map<T, typename std::list<T>::iterator> record;
};

} // namespace cmudb