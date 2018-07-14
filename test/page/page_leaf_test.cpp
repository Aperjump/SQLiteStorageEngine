/* 
 * page_leaf_test.cpp
 */
#include <algorithm>
#include <cstdio>
#include <sstream>
#include <list>
#include <unordered_map>
#include "page/b_plus_tree_internal_page.h"
#include "page/b_plus_tree_leaf_page.h"
#include "vtable/virtual_table.h"
#include "gtest/gtest.h"

namespace cmudb {

TEST(LeafNodeTests, InsertTest1) {
    Schema* key_schema = ParseCreateStatement("a bigint");
    GenericComparator<4> comparator(key_schema);

    DiskManager* disk_manager = new DiskManager("test.db");
    BufferPoolManager* bpm = new BufferPoolManager(50, disk_manager);
    GenericKey<4> index_key;
    RID rid;
    //Transaction* tnx = new Transaction(0);

    page_id_t page_id;
    Page* parent_page = bpm->NewPage(page_id);
    std::vector<int64_t> keys = {1,2,3,4,5};
    page_id_t leaf_page_id;
    Page* leaf_page = bpm->NewPage(leaf_page_id);
    BPlusTreeLeafPage<GenericKey<4>,RID,GenericComparator<4>>* leaf = new BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>(); 
    leaf->Init(leaf_page_id);
    /* start test */
    int counter = 0;
    for (auto key: keys) {
        int64_t value = key & 0xFFFF;
        rid.Set((int32_t)(key >> 16), value);
        index_key.SetFromInteger(key);
        leaf->Insert(index_key, rid, comparator);
        counter++;
        EXPECT_EQ(leaf->GetSize(), counter);
    }
    delete leaf;
    //delete tnx;
    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
}
}
