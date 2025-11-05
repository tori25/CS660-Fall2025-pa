#include <db/IndexPage.hpp>
#include <stdexcept>
#include <cstring>

using namespace db;

IndexPage::IndexPage(Page &page) {
  // TODO pa2

  header = reinterpret_cast<IndexPageHeader*>(page.data());

  // Keys come after the header
  keys = reinterpret_cast<int*>(page.data() + sizeof(IndexPageHeader));

  // Calculate capacity based on remaining space after header
  capacity = (DEFAULT_PAGE_SIZE - sizeof(IndexPageHeader) - sizeof(size_t)) / (sizeof(int) + sizeof(size_t));

  // Children array comes after all possible keys
  children = reinterpret_cast<size_t*>(page.data() + sizeof(IndexPageHeader) + capacity * sizeof(int));
}

bool IndexPage::insert(int key, size_t child) {
  // TODO pa2
  // Check if page is already full
  if (header->size >= capacity) {
    return true; // Page is full, needs split
  }

  size_t size = header->size;

  // Find insertion position (keys are sorted)
  size_t pos = 0;
  while (pos < size && keys[pos] < key) {
    pos++;
  }

  // Shift keys to make room
  for (size_t i = size; i > pos; i--) {
    keys[i] = keys[i - 1];
  }

  // Shift children to make room
  for (size_t i = size + 1; i > pos + 1; i--) {
    children[i] = children[i - 1];
  }

  // Insert new key and its right child pointer
  keys[pos] = key;
  children[pos + 1] = child;

  // Update header
  header->size++;

  // Return true if page is now full after insertion
  return header->size == capacity;
}

int IndexPage::split(IndexPage &new_page) {
  // TODO pa2
  // Split at midpoint
  size_t mid = header->size / 2;
  int split_key = keys[mid];

  size_t right_count = header->size - mid - 1;

  for (size_t i = 0; i < right_count; ++i)
    new_page.keys[i] = keys[mid + 1 + i];

  for (size_t i = 0; i < right_count + 1; ++i)
    new_page.children[i] = children[mid + 1 + i];

  new_page.header->size = right_count;
  header->size = mid;

  return split_key;
}
