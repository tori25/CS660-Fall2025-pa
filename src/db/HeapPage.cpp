#include <db/Database.hpp>
#include <db/HeapPage.hpp>
#include <stdexcept>
#include <cstring>

using namespace db;

HeapPage::HeapPage(Page &page, const TupleDesc &td) : td(td) {
    // TODO pa1

    // Compute capacity considering both tuple data and header must fit.
    size_t tupleSize = td.length();
    capacity = (DEFAULT_PAGE_SIZE * 8) / (tupleSize * 8 + 1); // initial upper bound
    while ((capacity * tupleSize + (capacity + 7) / 8) > DEFAULT_PAGE_SIZE) {
        capacity--;
    }
    size_t headerBytes = (capacity + 7) / 8;
    header = page.data();
    // Leave the padding between header and data so that tuples are packed at end
    size_t usedBytes = capacity * tupleSize + headerBytes;
    size_t padding = DEFAULT_PAGE_SIZE - usedBytes;
    // Data begins after header plus padding region.
    data = page.data() + headerBytes + padding;
}

size_t HeapPage::begin() const {
    // TODO pa1

    // Scan for first used slot
    for (size_t i = 0; i < capacity; i++) {
        uint8_t byte = header[i / 8];
        size_t bitInByte = 7 - (i % 8); // MSB-first
        bool used = (byte >> bitInByte) & 0x1;
        if (used) return i;
    }
    return capacity; // empty page
}

size_t HeapPage::end() const {
    // TODO pa1
    return capacity;
}

bool HeapPage::insertTuple(const Tuple &t) {
    // TODO pa1
    if (!td.compatible(t)) throw std::invalid_argument("HeapPage::insertTuple: tuple incompatible with schema");
    size_t tupleSize = td.length();
    for (size_t i = 0; i < capacity; i++) {
        uint8_t &byte = header[i / 8];
        size_t bitInByte = 7 - (i % 8);
        bool used = (byte >> bitInByte) & 0x1;
        if (!used) {
            byte |= (1u << bitInByte); // mark used
            td.serialize(data + i * tupleSize, t);
            return true;
        }
    }
    return false; // full
}

void HeapPage::deleteTuple(size_t slot) {
    // TODO pa1
    if (slot >= capacity) throw std::out_of_range("HeapPage::deleteTuple: slot index out of range");
    uint8_t &byte = header[slot / 8];
    size_t bitInByte = 7 - (slot % 8);
    bool used = (byte >> bitInByte) & 0x1;
    if (!used) throw std::logic_error("HeapPage::deleteTuple: slot already empty");
    byte &= ~(1u << bitInByte); // mark free
    size_t tupleSize = td.length();
    std::memset(data + slot * tupleSize, 0, tupleSize);
}

Tuple HeapPage::getTuple(size_t slot) const {
    // TODO pa1
    if (slot >= capacity) throw std::out_of_range("HeapPage::getTuple: slot index out of range");
    uint8_t byte = header[slot / 8];
    size_t bitInByte = 7 - (slot % 8);
    bool used = (byte >> bitInByte) & 0x1;
    if (!used) throw std::logic_error("HeapPage::getTuple: slot is empty");
    return td.deserialize(data + slot * td.length());
}

void HeapPage::next(size_t &slot) const {
    // TODO pa1
    for (size_t i = slot + 1; i < capacity; i++) {
        uint8_t byte = header[i / 8];
        size_t bitInByte = 7 - (i % 8);
        bool used = (byte >> bitInByte) & 0x1;
        if (used) { slot = i; return; }
    }
    slot = capacity; // end
}

bool HeapPage::empty(size_t slot) const {
    // TODO pa1
    if (slot >= capacity) throw std::out_of_range("HeapPage::empty: slot index out of range");
    uint8_t byte = header[slot / 8];
    size_t bitInByte = 7 - (slot % 8);
    return ((byte >> bitInByte) & 0x1) == 0;
}
