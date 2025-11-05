#include <db/Database.hpp>

using namespace db;

BufferPool &Database::getBufferPool() { return bufferPool; }

Database &db::getDatabase() {
    static Database instance;
    return instance;
}

void Database::add(std::unique_ptr<DbFile> file) {
    // TODO pa0
    const std::string &name = file->getName();
    if (files.contains(name)) {
        // If a file with this name already exists, remove it first
        // This handles the case where tests delete the physical file
        // but the Database still has the old file object registered
        bufferPool.flushFile(name);
        files.erase(name);
    }
    files[name] = std::move(file);
}

std::unique_ptr<DbFile> Database::remove(const std::string &name) {
    // TODO pa0
    auto nh = files.extract(name);
    if (nh.empty()) {
        throw std::logic_error("File does not exist");
    }
    Database::getBufferPool().flushFile(nh.key());
    return std::move(nh.mapped());
}

DbFile &Database::get(const std::string &name) const {
    // TODO pa0
    return *files.at(name);
}
