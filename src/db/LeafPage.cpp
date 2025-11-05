#include <db/LeafPage.hpp>
#include <stdexcept>
#include <cstring>

using namespace db;

LeafPage::LeafPage(Page &page, const TupleDesc &td, size_t key_index) : td(td), key_index(key_index) {
  // TODO pa2
  header = reinterpret_cast<LeafPageHeader*>(page.data());

  // Data starts after the header
  data = page.data() + sizeof(LeafPageHeader);

  // Calculate capacity based on remaining space and tuple size
  size_t available_space = DEFAULT_PAGE_SIZE - sizeof(LeafPageHeader);
  capacity = available_space / td.length();

}

bool LeafPage::insertTuple(const Tuple &t) {
  // TODO pa2
  // Get the key value from the tuple
  int key = std::get<int>(t.get_field(key_index));

  // Find insertion position (tuples are sorted by key)
  size_t pos = 0;
  for (size_t i = 0; i < header->size; i++) {
    Tuple existing = getTuple(i);
    int existing_key = std::get<int>(existing.get_field(key_index));

    if (existing_key == key) {
      // Key already exists, replace the tuple
      td.serialize(data + i * td.length(), t);
      return header->size >= capacity; // Return true if page is at capacity after update
    }

    if (existing_key > key) {
      break;
    }
    pos++;
  }

  // Check if we can insert (page not full)
  if (header->size >= capacity) {
    return true; // Page is full, cannot insert
  }

  // Shift tuples to make room for insertion
  size_t tuple_size = td.length();
  if (pos < header->size) {
    // Move tuples from pos onwards one position to the right
    memmove(data + (pos + 1) * tuple_size,
            data + pos * tuple_size,
            (header->size - pos) * tuple_size);
  }

  // Insert the new tuple
  td.serialize(data + pos * tuple_size, t);
  header->size++;

  // Return true if page is now at capacity
  return header->size >= capacity;
}

int LeafPage::split(LeafPage &new_page) {
  // TODO pa2
  // Split at midpoint
  size_t mid = header->size / 2;

  // Copy second half of tuples to new page
  size_t tuple_size = td.length();
  size_t new_size = header->size - mid;

  memcpy(new_page.data,
         data + mid * tuple_size,
         new_size * tuple_size);

  // Update the new page's next_leaf to point to the original page's next_leaf
  new_page.header->next_leaf = header->next_leaf;

  // Update sizes
  new_page.header->size = new_size;
  header->size = mid;

  // Return the split key (first key of new page)
  Tuple first_tuple = new_page.getTuple(0);
  return std::get<int>(first_tuple.get_field(key_index));
}

Tuple LeafPage::getTuple(size_t slot) const {
  // TODO pa2
  // Check bounds
  if (slot >= header->size) {
    throw std::out_of_range("Slot index out of range");
  }

  // Deserialize the tuple at the specified slot
  const uint8_t *tuple_data = data + slot * td.length();
  return td.deserialize(tuple_data);
}
