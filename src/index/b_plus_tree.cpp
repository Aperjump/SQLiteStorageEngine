/**
 * b_plus_tree.cpp
 */
#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"
#include "page/b_plus_tree_leaf_page.h"
#define B_PLUS_TREE_INTERNAL_PAGE_CUR_TYPE \
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>
namespace cmudb {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(const std::string &name,
                                BufferPoolManager *buffer_pool_manager,
                                const KeyComparator &comparator,
                                page_id_t root_page_id)
    : index_name_(name), root_page_id_(root_page_id),
      buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { 
    return root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                              std::vector<ValueType> &result,
                              Transaction *transaction) {
  auto leaf_node = FindLeafPage(key, false); 
  ValueType val;
  bool fi = leaf_node->Lookup(key, val, comparator_);
  if (fi)
      result.push_back(val);
  return fi;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                            Transaction *transaction) {
    if (IsEmpty()) {
        StartNewTree(key, value);
        return true;
    } else {
        return InsertIntoLeaf(key, value);
    }
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
    Page* cur_page = buffer_pool_manager_->NewPage(root_page_id_);
    if (cur_page == nullptr) {
        throw Exception(ExceptionType::EXCEPTION_TYPE_INVALID, "out of memory");
    }
    auto cur_page_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(cur_page->GetData());
    cur_page_node->Init(root_page_id_, INVALID_PAGE_ID);
    if (root_page_id_ != INVALID_PAGE_ID) {
        UpdateRootPageId(true);
    }
    else 
        buffer_pool_manager_->DeletePage(root_page_id_);
    cur_page_node->Insert(key, value, comparator_);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                    Transaction *transaction) {
    auto target_node = FindLeafPage(key, false);
    if (target_node == nullptr) return false;
    int cur_max_size = target_node->GetMaxSize();
    int new_size = target_node->Insert(key, value, comparator_);
    if (new_size > cur_max_size) {
        /* insert this node into a new node */
        B_PLUS_TREE_LEAF_PAGE_TYPE* cur_node = target_node;
        B_PLUS_TREE_LEAF_PAGE_TYPE* rep_node = Split(target_node);
        /* update parent node */
        rep_node->SetNextPageId(cur_node->GetNextPageId());
        cur_node->SetNextPageId(rep_node->GetPageId());
        InsertIntoParent(cur_node, rep_node->KeyAt(1), rep_node);
        buffer_pool_manager_->UnpinPage(rep_node->GetPageId(), true);
    }
    //buffer_pool_manager_->UnpinPage(target_node->GetPageId(), true);
    return true;
}
/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N> N *BPLUSTREE_TYPE::Split(N *node) { 
    /* allocate new page */
    page_id_t new_page_id;
    Page* new_page = buffer_pool_manager_->NewPage(new_page_id);
    if (new_page == nullptr)
        throw Exception(ExceptionType::EXCEPTION_TYPE_INVALID, "out of memory");
    /* get pointer type */
    N* new_node = reinterpret_cast<N*>(new_page->GetData());
    new_node->Init(new_page_id, node->GetParentPageId());
    node->MoveHalfTo(new_node, buffer_pool_manager_);
    return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                      const KeyType &key,
                                      BPlusTreePage *new_node,
                                      Transaction *transaction) {
    page_id_t parent_page_id = old_node->GetParentPageId();
    if (parent_page_id == INVALID_PAGE_ID) {
        /* create new root node */
        page_id_t new_root_id;
        Page* new_root_page = buffer_pool_manager_->NewPage(new_root_id);
        if (new_root_page == nullptr)
            throw Exception(ExceptionType::EXCEPTION_TYPE_INVALID, "out of memory");
        auto new_root_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_CUR_TYPE*>(new_root_page->GetData());
        new_root_node->Init(new_root_id, INVALID_PAGE_ID);
        root_page_id_ = new_root_id;
        LOG_INFO("New Parent Node: %d\n", new_root_id);
        page_id_t old_page_id = old_node->GetPageId();
        page_id_t new_page_id = new_node->GetPageId();
        new_root_node->PopulateNewRoot(old_page_id, key, new_page_id); 
        UpdateRootPageId(true);
        buffer_pool_manager_->UnpinPage(new_root_id, true);
        old_node->SetParentPageId(new_root_id);
        new_node->SetParentPageId(new_root_id);
        return;
    }
    Page* parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
    auto parent_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_CUR_TYPE*>(parent_page->GetData());
    page_id_t old_page_id = old_node->GetPageId();
    page_id_t new_node_id = new_node->GetPageId();
    int cur_parent_size = parent_node->InsertNodeAfter(old_page_id, key, new_node_id);
    int max_parent_size = parent_node->GetMaxSize();
    if (cur_parent_size <= max_parent_size) {
        buffer_pool_manager_->UnpinPage(parent_page_id, true);
        return;
    } else {
        auto new_parent_node = Split(parent_node);
        InsertIntoParent(parent_node, new_parent_node->KeyAt(1), new_parent_node, transaction);
        buffer_pool_manager_->UnpinPage(new_parent_node->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
    }
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
}


/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
    if (IsEmpty())
        return;
    auto leaf_page = FindLeafPage(key, false);
    if (leaf_page == nullptr)
        return;
    int cur_size =  leaf_page->RemoveAndDeleteRecord(key, comparator_);
    page_id_t parent_page_id = leaf_page->GetParentPageId();
    if (parent_page_id == INVALID_PAGE_ID) {
       // buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
        return;
    }
    if (cur_size < leaf_page->GetMinSize()) {
        /* needs to check whether delete the node or move one record from near node */ 
        CoalesceOrRedistribute(leaf_page, transaction);
    }
    //buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
}  

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
    page_id_t parent_id = node->GetParentPageId();
    if (parent_id == INVALID_PAGE_ID)
        AdjustRoot(node);
    int cur_size = node->GetSize();
    Page* parent_page = buffer_pool_manager_->FetchPage(parent_id);
    auto parent_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_CUR_TYPE*>(parent_page->GetData());
    int cur_index_in_parent = parent_node->ValueIndex(node->GetPageId());
    int parent_size = parent_node->GetSize();
    page_id_t sib_id;
    if (cur_index_in_parent == parent_size - 1)
        sib_id = parent_node->ValueAt(cur_index_in_parent - 1);
    else 
        sib_id = parent_node->ValueAt(cur_index_in_parent + 1);
    Page* sib_page = buffer_pool_manager_->FetchPage(sib_id);
    auto sib_node = reinterpret_cast<N*>(sib_page->GetData());
    int sib_size = sib_node->GetSize();
    bool redistri = false;
    if (sib_size + cur_size > node->GetMaxSize()) {
        redistri = true;
        buffer_pool_manager_->UnpinPage(parent_id, true);
    }
    if (redistri) {
        int redis_index = cur_index_in_parent == parent_size - 1 ? 1 : 0;
        Redistribute<N>(sib_node, node, redis_index);
        buffer_pool_manager_->UnpinPage(sib_id, true);
        return false;
    } else {
        bool fin_ret = false;
        if (cur_index_in_parent == parent_size - 1) {
            Coalesce<N>(sib_node, node, parent_node,cur_index_in_parent-1 , transaction);
            fin_ret = true;
        } else
            Coalesce<N>(sib_node, node, parent_node, cur_index_in_parent + 1, transaction);
        buffer_pool_manager_->UnpinPage(parent_id, true);
        buffer_pool_manager_->UnpinPage(sib_id, true);
        return fin_ret;
    }
    return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Coalesce(
    N *neighbor_node, N *node,
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent,
    int index, Transaction *transaction) {
    node->MoveAllTo(neighbor_node, index, buffer_pool_manager_);
    parent->Remove(index);
    if (parent->GetParentPageId() == INVALID_PAGE_ID) {
        return;
    }
    if (parent->GetSize() < parent->GetMinSize())
        CoalesceOrRedistribute(parent, transaction);
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
    if (index == 0) {
        node->MoveFirstToEndOf(neighbor_node, buffer_pool_manager_);
    } else {
        page_id_t parent_id = node->GetParentPageId();
        Page* parent_page = buffer_pool_manager_->FetchPage(parent_id);
        auto* parent_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_CUR_TYPE*>(parent_page->GetData());
        if (parent_id == INVALID_PAGE_ID)
            throw Exception(ExceptionType::EXCEPTION_TYPE_INVALID, "no valid node");
        int index_in_parent = parent_node->ValueIndex(node->GetParentPageId());
        buffer_pool_manager_->UnpinPage(parent_id, false);
        node->MoveLastToFrontOf(neighbor_node, index_in_parent, buffer_pool_manager_);
    }
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
    int old_root_size = old_root_node->GetSize();
    bool ret = false;
    if (old_root_size == 1) {
        auto* root_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_CUR_TYPE*>(old_root_node->GetPageId());
        root_page_id_ = root_node->ValueAt(0);
        UpdateRootPageId(false);
        Page* new_root_page = buffer_pool_manager_->FetchPage(root_page_id_);
        auto new_root_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_CUR_TYPE*>(new_root_page->GetData());
        new_root_node->SetParentPageId(INVALID_PAGE_ID);
        buffer_pool_manager_->UnpinPage(root_page_id_, true);
    } else if (old_root_size == 0) {
        old_root_node->SetParentPageId(INVALID_PAGE_ID);
        ret = true;
    }
    buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), true);
    return ret;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
    KeyType key{};
    return IndexIterator<KeyType, ValueType, KeyComparator>(
    FindLeafPage(key, true), 0, buffer_pool_manager_);
} /*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  auto *leaf = FindLeafPage(key, false);
  int index = 0;
  if (leaf != nullptr) {
    index = leaf->KeyIndex(key, comparator_);
  }
  return IndexIterator<KeyType, ValueType, KeyComparator>(
leaf, index, buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key,
                                                         bool leftMost,
                                                         Operation, Transaction*) {
    
    if (IsEmpty())
        throw Exception(ExceptionType::EXCEPTION_TYPE_INVALID, "No element in database");
     Page* root_page = buffer_pool_manager_->FetchPage(root_page_id_);
    if (root_page == nullptr)
        return nullptr;
    auto cur_node = reinterpret_cast<BPlusTreePage*>(root_page->GetData());
    Page* tmp_page = root_page;
    B_PLUS_TREE_INTERNAL_PAGE_CUR_TYPE* test;
    while (!cur_node->IsLeafPage()) {
        auto internal_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_CUR_TYPE*>(tmp_page->GetData());
        page_id_t tmp_page_id;
        if (leftMost)
            tmp_page_id = internal_node->ValueAt(0);
        else 
            tmp_page_id = internal_node->Lookup(key, comparator_);
        buffer_pool_manager_->UnpinPage(tmp_page->GetPageId(), false);
        tmp_page = buffer_pool_manager_->FetchPage(tmp_page_id);
        cur_node = reinterpret_cast<BPlusTreePage*>(tmp_page->GetData());
        test = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_CUR_TYPE*>(tmp_page->GetData());
    }
    auto leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(tmp_page->GetData());
    buffer_pool_manager_->UnpinPage(root_page_id_, false);
    buffer_pool_manager_->UnpinPage(cur_node->GetPageId(), false);
    return leaf_node;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(bool insert_record) {
  auto *page = buffer_pool_manager_->FetchPage(HEADER_PAGE_ID);
  if (page == nullptr) {
    throw Exception(EXCEPTION_TYPE_INDEX,
                    "all page are pinned while UpdateRootPageId");
  }
  auto *header_page = reinterpret_cast<HeaderPage *>(page->GetData());

  if (insert_record) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for debug only
 * print out whole b+tree sturcture, rank by rank
 */
INDEX_TEMPLATE_ARGUMENTS
std::string BPLUSTREE_TYPE::ToString(bool verbose) { return "Empty tree"; }

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;
} // namespace cmudb
