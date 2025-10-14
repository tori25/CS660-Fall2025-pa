#include <db/Database.hpp>
#include <db/HeapFile.hpp>
#include <db/HeapPage.hpp>
#include <stdexcept>

using namespace db;

HeapFile::HeapFile(const std::string &name, const TupleDesc &td) : DbFile(name, td) {}

void HeapFile::insertTuple(const Tuple &t) {
    // TODO pa1

    // Always attempt to insert into the last existing page first.
    if (getNumPages() > 0) {
        size_t lastPageId = getNumPages() - 1;
        Page page;
        readPage(page, lastPageId);
        HeapPage heapPage(page, td);
        if (heapPage.insertTuple(t)) {
            writePage(page, lastPageId);
            return;
        }
    }
    // Need a new page (either file empty or last page full).
    Page newPage{}; // zero-initialized
    HeapPage hp(newPage, td);
    if (!hp.insertTuple(t)) {
        throw std::runtime_error("HeapFile::insertTuple: failed to insert into fresh page");
    }
    // Append at id == numPages; writePage will extend numPages.
    writePage(newPage, getNumPages());
}

void HeapFile::deleteTuple(const Iterator &it) {
    // TODO pa1

    // Validate page index
    if (it.page >= getNumPages()) {
        throw std::out_of_range("HeapFile::deleteTuple: invalid page index");
    }

    // Read the corresponding page
    Page page;
    readPage(page, it.page);

    // Interpret it as a HeapPage
    HeapPage heapPage(page, td);

    // Delete the tuple at that slot
    heapPage.deleteTuple(it.slot);

    // Write updated page back to disk
    writePage(page, it.page);
}

Tuple HeapFile::getTuple(const Iterator &it) const {
    // TODO pa1

    // Verify the iterator belongs to this file
    if (&(it.file) != this) {
        throw std::invalid_argument("HeapFile::getTuple: iterator does not belong to this file");
    }

    // Validate page index
    if (it.page >= getNumPages()) {
        throw std::out_of_range("HeapFile::getTuple: invalid page index");
    }

    // Read the correct page
    Page page;
    readPage(page, it.page);

    // Wrap it in a HeapPage helper
    HeapPage heapPage(page, td);

    // Extract the tuple at the iterator’s slot
    return heapPage.getTuple(it.slot);
}

void HeapFile::next(Iterator &it) const {
    // TODO pa1

    // Check if iterator is already at end of file
    if (it.page >= getNumPages()) {
        return;  // already at end
    }

    // Read the current page
    Page page;
    readPage(page, it.page);
    HeapPage heapPage(page, td);

    // Move to the next populated slot in this page
    size_t slot = it.slot;
    heapPage.next(slot);

    // If still within this page, update iterator
    if (slot < heapPage.end()) {
        it.slot = slot;
        return;
    }

    // Otherwise, move to next page that has tuples
    for (size_t nextPage = it.page + 1; nextPage < getNumPages(); nextPage++) {
        Page next;
        readPage(next, nextPage);
        HeapPage nextHeap(next, td);

        size_t firstSlot = nextHeap.begin();
        if (firstSlot < nextHeap.end()) {
            it.page = nextPage;
            it.slot = firstSlot;
            return;
        }
    }

    // No more tuples → set iterator to end of file
    it.page = getNumPages();
    it.slot = 0;  // convention for EOF
}

Iterator HeapFile::begin() const {
    // TODO pa1

    for (size_t pageId = 0; pageId < getNumPages(); pageId++) {
        Page page;
        readPage(page, pageId);

        HeapPage heapPage(page, td);
        size_t firstSlot = heapPage.begin();

        if (firstSlot < heapPage.end()) {
            // Found first populated slot
            return Iterator{*this, pageId, firstSlot};
        }
    }

    // File empty — return end iterator
    return end();
}

Iterator HeapFile::end() const {
    // TODO pa1
    return Iterator{*this, getNumPages(), 0};
}
