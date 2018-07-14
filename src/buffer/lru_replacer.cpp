/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"
#include <mutex>
namespace cmudb {

template <typename T> LRUReplacer<T>::LRUReplacer() {}

template <typename T> LRUReplacer<T>::~LRUReplacer() {}

/*
 * Insert value into LRU
 */
template <typename T> void LRUReplacer<T>::Insert(const T &value) {
    std::lock_guard<std::mutex> guard(mut);
    auto item = record.find(value);
    if (item == record.end()) {
        auto tmpiter = val.insert(val.end(), value);
        record.insert(std::make_pair(value, tmpiter));
    } else {
        val.erase(item->second);
        auto tmpiter = val.insert(val.end(), value);
        record.erase(item);
        record.insert(std::make_pair(value,  tmpiter));
    }
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T> bool LRUReplacer<T>::Victim(T &value) {
     std::lock_guard<std::mutex> guard(mut);
     if (val.begin() == val.end()) {
         return false;
     } else {
         auto item = val.front();
         val.pop_front();
         value = item;
         record.erase(value);
         return true;
     }
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
    std::lock_guard<std::mutex> guard(mut);
    auto item = record.find(value);
    if (item == record.end())
        return false;
    else {
        auto item = record.find(value);
        val.erase(item->second);
        record.erase(item);
        return true;
    }
}

template <typename T> size_t LRUReplacer<T>::Size() { return val.size(); }

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace cmudb
