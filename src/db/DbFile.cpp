#include <db/DbFile.hpp>
#include <stdexcept>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

using namespace db;

const TupleDesc &DbFile::getTupleDesc() const { return td; }

DbFile::DbFile(const std::string &name, const TupleDesc &td) : name(name), td(td) {
    // TODO pa1: open file and initialize numPages
    // Hint: use open, fstat

    fd = open(name.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        throw std::runtime_error("Failed to open file: " + name);
    }

    struct stat st;
    if (fstat(fd, &st) < 0) {
        close(fd);
        throw std::runtime_error("Failed to stat file: " + name);
    }
    off_t fileSize = st.st_size;

    if (fileSize == 0) {
        // create an empty first page
        std::vector<char> buffer(DEFAULT_PAGE_SIZE, 0);
        if (pwrite(fd, buffer.data(), DEFAULT_PAGE_SIZE, 0) != DEFAULT_PAGE_SIZE) {
            close(fd);
            throw std::runtime_error("Failed to write first page: " + name);
        }
        numPages = 1;
    } else {
        numPages = fileSize / DEFAULT_PAGE_SIZE;
    }
}

DbFile::~DbFile() {
    // TODO pa1: close file
    // Hind: use close

    // Close only if we actually have an open descriptor.
    if (fd >= 0) {
        close(fd);
        fd = -1; // mark as invalid
    }
}

const std::string &DbFile::getName() const { return name; }

void DbFile::readPage(Page &page, const size_t id) const {
    reads.push_back(id);
    // TODO pa1: read page
    // Hint: use pread

    // Check that the page ID exists
    if (id >= numPages) {
        throw std::out_of_range("Page ID out of range");
    }

    // Calculate the byte offset in the file
    off_t offset = id * DEFAULT_PAGE_SIZE;

    // Read one page from the file
    ssize_t bytesRead = pread(fd, page.data(), DEFAULT_PAGE_SIZE, offset);

    // Verify that the entire page was read
    if (bytesRead != DEFAULT_PAGE_SIZE) {
        throw std::runtime_error("Failed to read full page " + std::to_string(id));
    }
}

void DbFile::writePage(const Page &page, const size_t id) const {
    writes.push_back(id);
    // TODO pa1: write page
    // Hint: use pwrite

    if (id > numPages) {
        throw std::out_of_range("Page ID out of range");
    }
    // Allow id == numPages to append a new page (HeapFile relies on this when growing).
    off_t offset = static_cast<off_t>(id) * static_cast<off_t>(DEFAULT_PAGE_SIZE);
    ssize_t bytesWritten = pwrite(fd, page.data(), DEFAULT_PAGE_SIZE, offset);

    if (bytesWritten != static_cast<ssize_t>(DEFAULT_PAGE_SIZE)) {
        throw std::runtime_error("Failed to write full page " + std::to_string(id));
    }
    if (id == numPages) {
        // update mutable member (cast away const for logical constness of method signature)
        auto *self = const_cast<DbFile *>(this);
        self->numPages++;
    }

}

const std::vector<size_t> &DbFile::getReads() const { return reads; }

const std::vector<size_t> &DbFile::getWrites() const { return writes; }

void DbFile::insertTuple(const Tuple &t) { throw std::runtime_error("Not implemented"); }

void DbFile::deleteTuple(const Iterator &it) { throw std::runtime_error("Not implemented"); }

Tuple DbFile::getTuple(const Iterator &it) const { throw std::runtime_error("Not implemented"); }

void DbFile::next(Iterator &it) const { throw std::runtime_error("Not implemented"); }

Iterator DbFile::begin() const { throw std::runtime_error("Not implemented"); }

Iterator DbFile::end() const { throw std::runtime_error("Not implemented"); }

size_t DbFile::getNumPages() const { return numPages; }
