#include <db/BufferPool.hpp>
#include <db/Database.hpp>
#include <numeric>

using namespace db;

BufferPool::BufferPool() {
    // TODO pa0
    used.fill(false);
    dirty.fill(false);
}

BufferPool::~BufferPool() {
    // TODO pa0
    std::vector<PageId> toFlush;
    toFlush.reserve(pageTable.size());

    for (const auto &pid : toFlush) {
        flushPage(pid);
    }
}

Page &BufferPool::getPage(const PageId &pid) {
    // TODO pa0
    if (auto it = pageTable.find(pid); it != pageTable.end()) {  // case 1- already cached
        size_t slot = it->second;

        lru.remove(slot);         // refresh LRU
        lru.push_front(slot);
        return pages[slot];
    }

    size_t slot;
    if (auto freeIt = std::find(used.begin(), used.end(), false); // case 2- free slot? evict one
        freeIt != used.end()) {

        slot = std::distance(used.begin(), freeIt); // unused slot found
    } else {
        slot = lru.back(); // evict least recently used
        lru.pop_back();

        auto victimIt = std::find_if(         // find victim PageId
            pageTable.begin(), pageTable.end(),
            [slot](const auto &p) { return p.second == slot; }
        );

        const PageId &victim = victimIt->first;

        if (dirty[slot]) {
            getDatabase().get(victim.file).writePage(pages[slot], victim.page);  // flush if dirty
            dirty[slot] = false;
        }

        pageTable.erase(victimIt);  // remove victim mapping
        used[slot] = false;
    }

    getDatabase().get(pid.file).readPage(pages[slot], pid.page);     // case 3: Load requested page from disk
    used[slot] = true;
    dirty[slot] = false;
    pageTable[pid] = slot;

    lru.remove(slot);
    lru.push_front(slot);

    return pages[slot];
}

void BufferPool::markDirty(const PageId &pid) {
    auto it = pageTable.find(pid);
    if (it == pageTable.end()) {
        throw std::logic_error("Page not found in buffer pool");
    }
    dirty[it->second] = true;
}

bool BufferPool::isDirty(const PageId &pid) const {
    // TODO pa0
    auto it = pageTable.find(pid);
    if (it == pageTable.end()) {
        throw std::logic_error("isDirty: Page not found in buffer pool");
    }
    return dirty[it->second];
}

bool BufferPool::contains(const PageId &pid) const {
    // TODO pa0
    return pageTable.find(pid) != pageTable.end();
}

void BufferPool::discardPage(const PageId &pid) {
    // TODO pa0
    auto it = pageTable.find(pid);
    if (it == pageTable.end()) return;

    size_t slot = it->second;
    used[slot] = false;
    dirty[slot] = false;

    pageTable.erase(it);
    lru.remove(slot);
}

void BufferPool::flushPage(const PageId &pid) {
    // TODO pa0
    auto it = pageTable.find(pid);
    if (it == pageTable.end()) return;

    size_t slot = it->second;
    if (dirty[slot]) {
        getDatabase().get(pid.file).writePage(pages[slot], pid.page);
        dirty[slot] = false;
    }
}

void BufferPool::flushFile(const std::string &file) {
    // TODO pa0
    std::vector<PageId> toFlush;
    for (const auto &entry : pageTable) {
        if (entry.first.file == file) {
            toFlush.push_back(entry.first);
        }
    }
    for (const auto &pid : toFlush) {
        flushPage(pid);
    }
}