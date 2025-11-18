#include <cstring>
#include <db/BTreeFile.hpp>
#include <db/Database.hpp>
#include <db/IndexPage.hpp>
#include <db/LeafPage.hpp>
#include <stdexcept>

using namespace db;

BTreeFile::BTreeFile(const std::string &name, const TupleDesc &td, size_t key_index)
    : DbFile(name, td), key_index(key_index) {}

void BTreeFile::insertTuple(const Tuple &t) {
  // TODO pa2
  // Struct to return split info from recursion
  struct SplitInfo { bool occurred; int key; size_t new_page; };

  // Recursive lambda to insert and handle splits
  std::function<SplitInfo(size_t)> insertRec = [&](size_t pid) -> SplitInfo {
    BufferPool &bp = getDatabase().getBufferPool();
    Page &page = bp.getPage({name, pid});
    IndexPage index(page);
    int key = std::get<int>(t.get_field(key_index));

    // Find which child to follow based on key
    size_t child_idx = 0;
    for (size_t i = 0; i < index.header->size; i++) {
      if (key < index.keys[i]) break;
      child_idx = i + 1;
    }

    // Base case: children are leaves
    if (!index.header->index_children) {
      size_t leaf_id = index.children[child_idx];
      Page &lf = bp.getPage({name, leaf_id});
      LeafPage leaf(lf, td, key_index);

      // Try insert into leaf
      if (!leaf.insertTuple(t)) {
        bp.markDirty({name, leaf_id});
        return {false, 0, 0}; // No split needed
      }

      // Leaf is full - split it
      size_t new_id = numPages++;
      Page &new_lf = bp.getPage({name, new_id});
      LeafPage new_leaf(new_lf, td, key_index);
      new_leaf.header->size = 0;
      new_leaf.header->next_leaf = leaf.header->next_leaf;
      int split_key = leaf.split(new_leaf);
      leaf.header->next_leaf = new_id;
      bp.markDirty({name, leaf_id});
      bp.markDirty({name, new_id});

      // Try insert split key into this index
      if (!index.insert(split_key, new_id)) {
        bp.markDirty({name, pid});
        return {false, 0, 0}; // Fit in index, done
      }

      // Index is full too - split it
      size_t new_idx = numPages++;
      IndexPage new_index(bp.getPage({name, new_idx}));
      new_index.header->size = 0;

      // Inherit whether the children are leaf pages
      new_index.header->index_children = index.header->index_children;

      int idx_key = index.split(new_index);
      bp.markDirty({name, pid});
      bp.markDirty({name, new_idx});
      return {true, idx_key, new_idx}; // Propagate split up
    }

    // Recursive case: children are index pages
    SplitInfo child = insertRec(index.children[child_idx]);
    if (!child.occurred) return {false, 0, 0}; // No split below

    // Try insert promoted key from child split
    if (!index.insert(child.key, child.new_page)) {
      bp.markDirty({name, pid});
      return {false, 0, 0}; // Fit in index, done
    }

    // This index is full - split it
    size_t new_idx = numPages++;
    IndexPage new_index(bp.getPage({name, new_idx}));
    new_index.header->size = 0;

    //Inherit the same type of children as the current index
    new_index.header->index_children = index.header->index_children;

    int split_key = index.split(new_index);
    bp.markDirty({name, pid});
    bp.markDirty({name, new_idx});
    return {true, split_key, new_idx}; // Propagate split up
  };

  BufferPool &bp = getDatabase().getBufferPool();

  // Initialize empty tree with root and first leaf
  if (numPages == 1) {
    IndexPage root(bp.getPage({name, root_id}));
    root.header->size = 0;
    root.header->index_children = false;

    size_t leaf_id = numPages++;  // leaf_id = 1, numPages becomes 2
    LeafPage leaf(bp.getPage({name, leaf_id}), td, key_index);
    leaf.header->size = 0;
    leaf.header->next_leaf = 0;
    leaf.insertTuple(t);
    root.children[0] = leaf_id;
    bp.markDirty({name, root_id});
    bp.markDirty({name, leaf_id});
    return;
  }

  // Start recursive insert from root
  SplitInfo split = insertRec(root_id);
  if (!split.occurred) return; // No split at root

  // Root split - copy root to new left child, update root
  IndexPage root(bp.getPage({name, root_id}));
  size_t left_id = numPages++;
  IndexPage left(bp.getPage({name, left_id}));
  left.header->size = root.header->size;
  left.header->index_children = root.header->index_children;
  for (size_t i = 0; i <= root.header->size; i++) {
    if (i < root.header->size) left.keys[i] = root.keys[i];
    left.children[i] = root.children[i];
  }
  bp.markDirty({name, left_id});

  // Root now just points to left and right children
  root.header->size = 1;
  root.header->index_children = true;
  root.keys[0] = split.key;
  root.children[0] = left_id;
  root.children[1] = split.new_page;
  bp.markDirty({name, root_id});
  std::vector<size_t> path;
  BufferPool &bufferPool = getDatabase().getBufferPool();
  PageId pid{name, root_id};

  Page &root_page = bufferPool.getPage(pid);
  IndexPage root(root_page);
  if (root.header->size == 0 && root.children[0] != 1) {
    bufferPool.markDirty({name, root_id});
    pid.page = numPages++;
    root.children[0] = pid.page;
  } else {
    while (true) {
      Page &page = bufferPool.getPage(pid);
      IndexPage node(page);
      auto pos = std::lower_bound(node.keys, node.keys + node.header->size, std::get<int>(t.get_field(key_index)));
      auto slot = pos - node.keys;
      pid.page = node.children[slot];
      if (!node.header->index_children) {
        break;
      }
      path.push_back(pid.page);
    }
  }

  Page &page = bufferPool.getPage(pid);
  bufferPool.markDirty(pid);
  LeafPage leaf(page, td, key_index);
  if (!leaf.insertTuple(t)) {
    return;
  }

  pid.page = numPages++;
  Page &new_leaf_page = bufferPool.getPage(pid);
  bufferPool.markDirty(pid);
  LeafPage new_leaf(new_leaf_page, td, key_index);
  int new_key = leaf.split(new_leaf);
  leaf.header->next_leaf = pid.page;
  size_t new_child = pid.page;

  while (!path.empty()) {
    size_t parent_id = path.back();
    path.pop_back();
    pid.page = parent_id;
    Page &parent_page = bufferPool.getPage(pid);
    bufferPool.markDirty(pid);
    IndexPage parent(parent_page);
    if (!parent.insert(new_key, new_child)) {
      return;
    }

    pid.page = numPages++;
    Page &new_internal_page = bufferPool.getPage(pid);
    bufferPool.markDirty(pid);
    IndexPage new_internal(new_internal_page);
    new_key = parent.split(new_internal);
    new_child = pid.page;
  }

  bufferPool.markDirty({name, root_id});
  if (!root.insert(new_key, new_child)) {
    return;
  }
  pid.page = numPages++;
  Page &new_child1 = bufferPool.getPage(pid);
  bufferPool.markDirty(pid);
  size_t child1 = pid.page;
  new_child1 = root_page;
  IndexPage child1_page(new_child1);

  pid.page = numPages++;
  Page &new_child2 = bufferPool.getPage(pid);
  bufferPool.markDirty(pid);
  size_t child2 = pid.page;
  IndexPage child2_page(new_child2);

  int key = child1_page.split(child2_page);
  root.header->size = 1;
  root.header->index_children = true;
  root.keys[0] = key;
  root.children[0] = child1;
  root.children[1] = child2;
}

void BTreeFile::deleteTuple(const Iterator &it) {
  // Do not implement
}

Tuple BTreeFile::getTuple(const Iterator &it) const {
  // TODO pa2
  // Get the page from buffer pool
  BufferPool &bp = getDatabase().getBufferPool();
  Page &page = bp.getPage({name, it.page});

  // Wrap page as LeafPage and get tuple at slot
  LeafPage leaf(page, td, key_index);
  return leaf.getTuple(it.slot);
  BufferPool &bufferPool = getDatabase().getBufferPool();
  PageId pid{name, it.page};
  Page &page = bufferPool.getPage(pid);
  LeafPage leaf(page, td, key_index);
  return leaf.getTuple(it.slot);
}

void BTreeFile::next(Iterator &it) const {
  // TODO pa2
  // Get current leaf page
  BufferPool &bp = getDatabase().getBufferPool();
  Page &page = bp.getPage({name, it.page});
  LeafPage leaf(page, td, key_index);

  // Move to next slot
  it.slot++;

  // If still within current page, done
  if (it.slot < leaf.header->size) {
    return;
  }

  // Move to next leaf page
  if (leaf.header->next_leaf != 0) {
    it.page = leaf.header->next_leaf;
    it.slot = 0;
  } else {
    // No more pages - set to end
    Iterator end_it = end();
    it.page = end_it.page;
    it.slot = end_it.slot;
  }
  BufferPool &bufferPool = getDatabase().getBufferPool();
  PageId pid{name, it.page};
  Page &page = bufferPool.getPage(pid);
  LeafPage leaf(page, td, key_index);
  if (it.slot + 1 < leaf.header->size) {
    it.slot++;
  } else {
    it.page = leaf.header->next_leaf;
    it.slot = 0;
  }
}

Iterator BTreeFile::begin() const {
  BufferPool &bufferPool = getDatabase().getBufferPool();
  PageId pid{name, root_id};
  while (true) {
    Page &page = bufferPool.getPage(pid);
    IndexPage node(page);
    pid.page = node.children[0];
    if (!node.header->index_children) {
      break;
    }
  }
  return {*this, pid.page, 0};
  // TODO pa2
  // If empty tree, return end
  if (numPages == 1) {
    return end();
  }

  BufferPool &bp = getDatabase().getBufferPool();
  size_t current = root_id;

  // Traverse down to leftmost leaf
  while (true) {
    Page &page = bp.getPage({name, current});
    IndexPage index(page);

    // If children are leaves, found the leftmost leaf
    if (!index.header->index_children) {
      current = index.children[0];
      break;
    }

    // Go to leftmost child
    current = index.children[0];
  }

  // Check if leaf has tuples
  Page &leaf_page = bp.getPage({name, current});
  LeafPage leaf(leaf_page, td, key_index);

  if (leaf.header->size == 0) {
    return end();
  }

  return Iterator(*this, current, 0);
}

Iterator BTreeFile::end() const {
  return {*this, 0, 0};
}
