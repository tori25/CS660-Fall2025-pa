#pragma once
#include <db/DbFile.hpp>
#include <algorithm>

//FakeDbFile that Stores name like "file", "0",
//Impl. readPage(Page&, size_t) by just filling the page, writePage(Page&, size_t) as a no-op or logs the write, Reports getName(), getNumPages() safely

namespace db {

    class StubDbFile : public DbFile {
    public:
        explicit StubDbFile(const std::string &name)
            : DbFile(name, TupleDesc{}) {}

        // Not override (base methods are not virtual)
        void readPage(Page &page, size_t id) const {
            std::fill(page.begin(), page.end(), static_cast<uint8_t>(id));
            // record the read
            const_cast<std::vector<size_t>&>(getReads()).push_back(id);
        }

        void writePage(const Page & /*page*/, size_t id) const {
            // record the write
            const_cast<std::vector<size_t>&>(getWrites()).push_back(id);
        }
    };

} // namespace db