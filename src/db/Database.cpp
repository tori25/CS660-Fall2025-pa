#include <db/Database.hpp>

using namespace db;

BufferPool &Database::getBufferPool() { return bufferPool; }

Database &db::getDatabase() {
    static Database instance;
    return instance;
}

void Database::add(std::unique_ptr<DbFile> file) {
    const std::string &name = file->getName();

    // If already exists, throw (tests expect this)
    if (files.find(name) != files.end()) {
        throw std::invalid_argument("File already exists in database: " + name);
    }

    files[name] = std::move(file);
}

std::unique_ptr<DbFile> Database::remove(const std::string &name) {
    // 1. Check if file exists
    auto it = files.find(name);
    if (it == files.end()) {
        throw std::logic_error("File with name '" + name + "' does not exist.");
    }

    // 2. Flush dirty pages for this file (important!)
    bufferPool.flushFile(name);

    // 3. Transfer ownership back to caller
    std::unique_ptr<DbFile> removed = std::move(it->second);

    // 4. Remove from catalog
    files.erase(it);

    return removed;
}

DbFile &Database::get(const std::string &name) const {
    // TODO pa0
    auto it = files.find(name);
    if (it == files.end()) {
        throw std::logic_error("File with name '" + name + "' does not exist.");
    }
    return *(it->second);

}