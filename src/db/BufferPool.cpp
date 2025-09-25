#include <db/BufferPool.hpp>
#include <db/Database.hpp>
#include <numeric>
#include <iostream>

using namespace db;

void BufferPool::clear() {
    pages.clear();
    dirtyPages.clear();
    lru.clear();
}

BufferPool::BufferPool( size_t capacity) : capacity(capacity){
    // TODO pa0
}


BufferPool::~BufferPool() {
    // TODO pa0
}

Page &BufferPool::getPage(const PageId &pid) {
    // TODO pa0

    if (pages.contains(pid)) { // page  cached? return it

        lru.remove(pid); // update LRU - move to front
        lru.push_front(pid);
        return pages[pid];
    }

    if (pages.size() >= capacity) {
        const PageId victim = lru.back();
        lru.pop_back();

        // Always flush the evicted page before removing it
        flushPage(victim);

        pages.erase(victim);
        dirtyPages.erase(victim);
    }

    // Load from disk
    Page page;
    getDatabase().get(pid.file).readPage(page, pid.page);


    // Insert into cache
    pages[pid] = page;
    lru.push_front(pid);

    return pages[pid];
}

void BufferPool::markDirty(const PageId &pid) {
    auto it = pages.find(pid);
    if (it == pages.end()) {
        throw std::logic_error("Page not found in buffer pool");
    }

    dirtyPages.insert(pid); // Mark as dirty

}

bool BufferPool::isDirty(const PageId &pid) const {
    // TODO pa0

    if (!pages.contains(pid)) {
        throw std::logic_error("Page not found in buffer pool");
    }
    return dirtyPages.find(pid) != dirtyPages.end();
}

bool BufferPool::contains(const PageId &pid) const {
    // TODO pa0
    return pages.find(pid) != pages.end();
}

void BufferPool::discardPage(const PageId &pid) {
    pages.erase(pid);
    lru.remove(pid);
    // Keep dirtyPages entry so flushFile can later write it.
}

void BufferPool::flushPage(const PageId &pid) {
    auto it = pages.find(pid);
    if (it == pages.end()) return;

    if (dirtyPages.contains(pid)) {
        getDatabase().get(pid.file).writePage(it->second, pid.page);
        dirtyPages.erase(pid);
    }
}

void BufferPool::flushFile(const std::string &file) {
    // TODO pa0
    for (auto it = pages.begin(); it != pages.end(); ++it) {
        if (it-> first.file == file) {
            flushPage(it-> first);
        }
    }
}
