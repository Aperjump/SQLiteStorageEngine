/**
 * b_plus_tree_internal_page.cpp
 */
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "page/b_plus_tree_internal_page.h"

namespace cmudb {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id,
                                          page_id_t parent_id) {
    SetPageType(IndexPageType::INTERNAL_PAGE);
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetSize(0);
    /* max_size should not take invalid node into consideration */
    int max_size  = (PAGE_SIZE - sizeof(BPlusTreeInternalPage)) / sizeof(MappingType) - 1;
    SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
    array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for (int i = 0; i < GetSize(); i++) {
      if (array[i].second == value)
          return i;
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const { 
    return array[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType
B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key,
                                       const KeyComparator &comparator) const {
  for (int i = 1; i < GetSize(); i++) {
      if (comparator(array[i].first, key) == 0)
        return array[i].second;
  }
  /* cannot find key */
  return ValueType();
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
/* This function only creates a new root page */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
    array[0].second = old_value;
    array[1] = std::make_pair(new_key, new_value);
    IncreaseSize(1);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */

/* In B+ tree code, after internal node calling InsertNodeAfter, B+ tree should retest
 * it's current size, if the size is equal to MaxSize, it next call MoveHalfTo */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
    assert(GetSize() + 1 <= GetMaxSize());
    for (int i = GetSize(); i > 0; i--) {
        if (array[i - 1].second == old_value) {
            array[i] = std::make_pair(new_key, new_value);
            IncreaseSize(1);
            break;
        }
        array[i] = array[i-1];
    }
    IncreaseSize(1);
    return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
/* B+ tree should allocate memory for recipient first */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
    int half = GetSize() / 2;
    /* set the page_0 to the final node in original node */
    recipient->array[0].second = array[half].second;
    recipient->CopyHalfFrom(array + half, GetSize() -  half, buffer_pool_manager);
    IncreaseSize(half * -1);
    /* Note: maptype is <key, page_id>, each page_id points to a inner_node or leaf_node */
    for (int index = half; index < GetSize(); index++) {
        ValueType rec_page_id = ValueAt(index);  
        Page* tmp_page = buffer_pool_manager->FetchPage(rec_page_id);
        assert(tmp_page != nullptr);
        /* cannot dicide whether the node is internal or leaf */
        auto tmp_node = reinterpret_cast<BPlusTreePage*>(tmp_page->GetData());
        tmp_node->SetParentPageId(recipient->GetPageId());
        buffer_pool_manager->UnpinPage(rec_page_id, false);
    }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
    /* This node should have size 0 */
    assert(GetSize() == 0);
    for (int i = 1; i < size; i ++) {
        array[i] = items[i-1];
    }
    IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
    for (int i = index; i < GetMaxSize() - 1; i++) {
        array[index] = array[index+1];
    }
    IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
    ValueType ret = array[0].second;
    SetSize(0);
    return ret;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(
    BPlusTreeInternalPage *recipient, int index_in_parent,
    BufferPoolManager *buffer_pool_manager) {
    int cur_size = GetSize();
    int rep_size = recipient->GetSize();
    int rep_max_size = recipient->GetMaxSize();
    assert(cur_size + rep_size <= rep_max_size);
    /* need parent_node to delete this page*/
    page_id_t parent_page_id = GetParentPageId();
    Page* parent_page = buffer_pool_manager->FetchPage(parent_page_id);
    auto* parent_buf_page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(parent_page->GetData());
    parent_buf_page->Remove(index_in_parent);
    /* The zero node is same as the last node of recipient, no need to copy */
    recipient->CopyAllFrom(array + 1, cur_size , buffer_pool_manager);
    page_id_t rep_page_id = recipient->GetPageId();
    for (int i = 1; i < cur_size; i++) {
        ValueType rec_page_id = array[i].second;
        Page* tmp_page = buffer_pool_manager->FetchPage(rec_page_id);
        auto* tmp_buf_page = reinterpret_cast<BPlusTreePage*>(tmp_page->GetData());
        tmp_buf_page->SetParentPageId(rep_page_id);
        buffer_pool_manager->UnpinPage(rec_page_id, true);
    }
    IncreaseSize(-1 * cur_size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
    int begin = GetSize();
    assert(begin + size <= GetMaxSize());
    for (int i = 1; i < size; i++) {
        array[begin + i] = items[i];
    }
    IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
    assert(recipient->GetSize() + 1 <= recipient->GetMaxSize());
    /* note: first node is dummy */
    MappingType new_pair(array[1].first, array[0].second);
    recipient->CopyLastFrom(new_pair, buffer_pool_manager);
    ValueType tmp_array_val = array[0].second;
    /* use array[1] key as dummy key */
    array[0].first = array[1].first;
    for (int i = 1; i < GetSize(); i++ )
        array[i] = array[i+1];
    IncreaseSize(-1);
    /* update parent node */
    auto parent_id = GetParentPageId();
    Page* tmp_parent_page = buffer_pool_manager->FetchPage(parent_id);
    auto* tmp_buf_page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(tmp_parent_page->GetData());
    int index_in_parent = tmp_buf_page->ValueIndex(tmp_array_val);
    tmp_buf_page->SetKeyAt(index_in_parent, array[0].first);
    buffer_pool_manager->UnpinPage(tmp_buf_page->GetParentPageId(), true);
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(
    const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
    assert(GetSize() + 1 <= GetMaxSize());
    array[GetSize()] = pair;
    IncreaseSize(1);
    ValueType new_page_id = pair.second;
    /* update child node */
    Page* tmp_child_page = buffer_pool_manager->FetchPage(new_page_id);
    auto* tmp_buf_page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(tmp_child_page->GetData());
    tmp_buf_page->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(tmp_buf_page->GetPageId(), true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeInternalPage *recipient, int parent_index,
    BufferPoolManager *buffer_pool_manager) {
    MappingType tmp_pair = array[GetSize()];
    page_id_t tmp_child = tmp_pair.second;
    recipient->CopyFirstFrom(tmp_pair, parent_index, buffer_pool_manager);
    Page* tmp_child_page = buffer_pool_manager->FetchPage(tmp_child);
    auto* tmp_child_buf = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(tmp_child_page->GetData());
    tmp_child_buf->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(tmp_child_buf->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(
    const MappingType &pair, int parent_index,
    BufferPoolManager *buffer_pool_manager) {
    assert(GetSize() +1 <= GetMaxSize());
    /* update parent */
    auto parent_id = GetParentPageId();
    Page* tmp_parent_page = buffer_pool_manager->FetchPage(parent_id);
    auto* tmp_buf_page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(tmp_parent_page->GetData());
    KeyType tmp_key = tmp_buf_page->KeyAt(parent_index);
    tmp_buf_page->SetKeyAt(parent_index, pair.first);
    buffer_pool_manager->UnpinPage(tmp_parent_page->GetPageId(), true);
    InsertNodeAfter(array[0].second, tmp_key, array[0].second);
    array[0].second = pair.second;
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::QueueUpChildren(
    std::queue<BPlusTreePage *> *queue,
    BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < GetSize(); i++) {
    auto *page = buffer_pool_manager->FetchPage(array[i].second);
    if (page == nullptr)
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while printing");
    BPlusTreePage *node =
        reinterpret_cast<BPlusTreePage *>(page->GetData());
    queue->push(node);
  }
}

INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_INTERNAL_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream os;
  if (verbose) {
    os << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
       << "]<" << GetSize() << "> ";
  }

  int entry = verbose ? 0 : 1;
  int end = GetSize();
  bool first = true;
  while (entry < end) {
    if (first) {
      first = false;
    } else {
      os << " ";
    }
    os << std::dec << array[entry].first.ToString();
    if (verbose) {
      os << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return os.str();
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t,
                                           GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t,
                                           GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t,
                                           GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t,
                                           GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t,
                                           GenericComparator<64>>;
} // namespace cmudb
