#include <db/Database.hpp>

using namespace db;

BufferPool &Database::getBufferPool()
{
    return bufferPool;
}

Database &db::getDatabase() {
    static Database instance;
    return instance;
}
void Database::add(std::unique_ptr<DbFile> file) {
    // TODO pa0
    const std::string &name = file->getName();  // Assuming DbFile has getName()

    // Check if this file already exists
    if (files.find(name) != files.end()) {
        throw std::logic_error("File with name '" + name + "' already exists.");
    }

    // Insert into the map (ownership transferred)
    files[name] = std::move(file);
}

std::unique_ptr<DbFile> Database::remove(const std::string &name) {
    // 1. Check if file exists
    auto it = files.find(name);
    if (it == files.end()) {
        throw std::logic_error("File with name '" + name + "' does not exist.");
    }

    // 2. Flush dirty pages of this file
    bufferPool.flushFile(name);

    // 3. Transfer ownership back to caller
    std::unique_ptr<DbFile> removed = std::move(it->second);

    // 4. Remove from catalog
    files.erase(it);

    return removed;
}

DbFile &Database::get(const std::string &name) const {
    auto it = files.find(name);
    if (it == files.end()) {
        throw std::logic_error("File with name '" + name + "' does not exist.");
    }
    return *(it->second);
}

void Database::clear() {
    files.clear();
    bufferPool.clear();   // reset contents without reassigning
}