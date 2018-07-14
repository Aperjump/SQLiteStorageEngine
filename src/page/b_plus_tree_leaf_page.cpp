/**
 * b_plus_tree_leaf_page.cpp
 */

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "common/logger.h"
#include "page/b_plus_tree_leaf_page.h"
#include "page/b_plus_tree_internal_page.h"
namespace cmudb {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id) {
    SetPageType(IndexPageType::LEAF_PAGE);
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetSize(0);
    /* remove header + next_page_id + array, remaining space only store MappingType */
    SetMaxSize((PAGE_SIZE - sizeof(BPlusTreeLeafPage)) / (sizeof(KeyType)+ sizeof(ValueType)));
    next_page_id_ = INVALID_PAGE_ID; 
    //if (ENABLE_LOGGING) {
    //LOG_INFO("#[Leaf Init]: create %d page", page_id);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
  return next_page_id_;
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
    next_page_id_ = next_page_id;
}


/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(
    const KeyType &key, const KeyComparator &comparator) const {
    int cur_size = GetSize();
    int i = 0;
    for (; i < cur_size; i++) {
        int compare_result = comparator(key, array[i].first);
        if (compare_result == -1 || compare_result == 0)
                break;
    }
    return i;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  KeyType key = { array[index].first };
  return key;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key,
                                       const ValueType &value,
                                       const KeyComparator &comparator) {
    /* do not consider overflow, if this happens, B+ tree will deal with 
     * boundary check */
    //int max_size = GetMaxSize();
    int keyid_to_replace = KeyIndex(key, comparator);
    if (comparator(KeyAt(keyid_to_replace), key) == 0)
        return GetSize();
    /*
    for (int i = cur_size; i > keyid_to_replace; i--)
        array[i] = array[i-1];
    */
    memmove(array+keyid_to_replace+1, array+keyid_to_replace, 
            (GetSize()-keyid_to_replace) * sizeof(MappingType));
    array[keyid_to_replace] = std::make_pair(key, value);
    IncreaseSize(1);
    //if (ENABLE_LOGGING) {
    //LOG_INFO("[Leaf Insert]: Leaf page %d : %s\n", GetPageId(), ToString().c_str());
    return GetSize(); 
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(
    BPlusTreeLeafPage *recipient,
    __attribute__((unused)) BufferPoolManager *buffer_pool_manager) {
    /* how many elements to move */
    int cur_size = GetSize();
    int left_half = cur_size / 2;
    recipient->CopyHalfFrom(&array[cur_size - left_half], left_half);
    //recipient->SetNextPageId(GetNextPageId());
    //SetNextPageId(recipient->GetPageId());
    IncreaseSize(-1 * left_half);
    /*
    LOG_INFO("[Leaf MoveHalfTo]: Leaf page %d : %s\n \
Leaf page %d : %s\n", GetPageId(), ToString().c_str(),
                recipient->GetPageId(), recipient->ToString().c_str());
    */
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalfFrom(MappingType *items, int size) {
    /* not sure whether this function can only be called from MoveHalfTp */
    for (int i = 0; i < size; i++)
        array[i] = items[i];
    IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value,
                                        const KeyComparator &comparator) const {
    int cur_size = GetSize();
    for (int i = 0; i < cur_size; i++) {
        if (comparator(array[i].first, key) == 0) {
            value = array[i].second;
            return true;
        }
    }
    return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immdiately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(
    const KeyType &key, const KeyComparator &comparator) {
    int cur_size = GetSize();
    // int max_size = GetMaxSize();
    /* do not consider merge problem here */
    int key_to_remove = KeyIndex(key, comparator);
    if (comparator(key, array[key_to_remove].first) == 0) {
        /* key exist */
        /*
        for (int i = key_to_remove; i < cur_size - 1; i++) {
            array[i] = array[i + 1];
        }
        */
        memmove(array+key_to_remove,array+key_to_remove+1, (cur_size-key_to_remove-1) * sizeof(MappingType));
        IncreaseSize(-1);
        return cur_size - 1;
    } else 
        return cur_size;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update next page id
 */
/* Here B+ tree should copy larger value to the back of small value */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient,
                                           int, BufferPoolManager *) {
    int cur_size = GetSize();
    recipient->CopyAllFrom(array, cur_size);
    recipient->SetNextPageId(GetNextPageId());
    /*
    LOG_INFO("[Leaf MoveAllTo] Leaf page %d : %s \n \
Leaf page %d : %s\n", GetPageId(), ToString().c_str(),
                recipient->GetPageId(), recipient->ToString().c_str());
    */
    /* reset parent page key */
    // Page* cur_page = buffer_pool_manager->FetchPage(cur_page_id);
    // auto* cur_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(cur_page->GetData());
    // page_id_t parent_id = cur_node->GetParentPageId();
    /*
    Page* parent_page = buffer_pool_manager->FetchPage(parent_id);
    auto* parent_node = reinterpret_cast<*>(parent_page->GetData());
    */
    /* find the node in parent containing current node key */
    /*
    int index_in_parent = parent_node->ValueIndex(cur_page_id);
    parent_node->Remove(index_in_parent);
    buffer_pool_manager->UnpinPage(parent_id, true);
    */
    /* get prev node and reset its next_page_id */
    /*
    if (prev_page_id_ != INVALID_PAGE_ID) {
        Page* prev_page = buffer_pool_manager->FetchPage(prev_page_id_);
        auto* prev_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(prev_page->GetData());
        prev_node->SetNextPageId(recipient->GetPageId());
        recipient->SetPrevPageId(prev_node->GetPageId());
        buffer_pool_manager->UnpinPage(prev_page_id_, true);
    }
    */
    /* There is another problem when updating neighbor nodes
     * We have to fetch via parent node, this is the design problem
     * in b_plus_tree_leaf_page data structure, from my point of view, 
     * it should add a prev_page_id */
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyAllFrom(MappingType *items, int size) {
    int cur_size = GetSize();
    assert(cur_size + size <= GetMaxSize());
    // int max_size = GetMaxSize();
    // assert(cur_size + size <= max_size);
    /* B+ tree is responsible to maintain the order */
    for (int i = 0; i < size; i++)
        array[cur_size + i] = *items++;
    IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
/* Do not implement these two functions */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeLeafPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
    MappingType tmp = GetItem(0);
    IncreaseSize(-1);
    /*
    for (int i = 0; i < GetSize(); i++)
        array[i] = array[i+1];
    */
    recipient->CopyLastFrom(tmp);
    memmove(array, array+1, GetSize() * sizeof(MappingType));
    page_id_t parent_id = GetParentPageId();
    Page* parent_page = buffer_pool_manager->FetchPage(parent_id);
    auto* parent_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(parent_page->GetData());
    parent_node->SetKeyAt(parent_node->ValueIndex(GetPageId()), tmp.first);
    buffer_pool_manager->UnpinPage(parent_id, true);
    /*
    LOG_INFO("[Leaf MoveFirstToEndOf] Leaf page %d : %s \n \
Leaf page %d : %s\n", GetPageId(), ToString().c_str(), 
                recipient->GetPageId(), recipient->ToString().c_str());
    */
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
    int cur_size = GetSize();
    assert(cur_size + 1 <= GetMaxSize());
    array[cur_size] = item;
    IncreaseSize(1);
}
/*
 * Remove the last key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
/* Do not implement these two functions */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeLeafPage *recipient, int ,
    BufferPoolManager *buffer_pool_manager) {
    int cur_size = GetSize();
    MappingType tmp = GetItem(cur_size-1);
    IncreaseSize(-1);
    page_id_t parent_id = GetParentPageId();
    Page* parent_node = buffer_pool_manager->FetchPage(parent_id);
    auto* parent_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(parent_node->GetData());
    int index = parent_page->ValueIndex(recipient->GetPageId());
    recipient->CopyFirstFrom(tmp, index, buffer_pool_manager); 
    /*
    LOG_INFO("[Leaf MoveFirstToEndOf] Leaf page %d : %s \n \
Leaf page %d : %s\n", GetPageId(), ToString().c_str(), 
                recipient->GetPageId(), recipient->ToString().c_str());
    */
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(
    const MappingType &item, int parentIndex,
    BufferPoolManager *buffer_pool_manager) {
    /*
    for (int i = cur_size; i > 0; i--)
        array[i] = array[i-1];
    */
    memmove(array+1, array, GetSize() * sizeof(MappingType));
    array[0] = item;

    page_id_t parent_id = GetParentPageId();
    Page* parent_page = buffer_pool_manager->FetchPage(parent_id);
    auto* parent_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(parent_page->GetData());
    parent_node->SetKeyAt(parentIndex, item.first);
    IncreaseSize(1);
    buffer_pool_manager->UnpinPage(parent_id, true);
}


/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_LEAF_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream stream;
  if (verbose) {
    stream << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
           << "]<" << GetSize() << "> ";
  }
  int entry = 0;
  int end = GetSize();
  bool first = true;

  while (entry < end) {
    if (first) {
      first = false;
    } else {
      stream << " ";
    }
    stream << std::dec << array[entry].first;
    if (verbose) {
      stream << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return stream.str();
}

template class BPlusTreeLeafPage<GenericKey<4>, RID,
                                       GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID,
                                       GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID,
                                       GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID,
                                       GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID,
                                       GenericComparator<64>>;
} // namespace cmudb
