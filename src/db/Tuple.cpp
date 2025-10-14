#include <cstring>
#include <db/Tuple.hpp>
#include <stdexcept>
#include <unordered_set>

using namespace db;

Tuple::Tuple(const std::vector<field_t> &fields) : fields(fields) {}

type_t Tuple::field_type(size_t i) const {
    const field_t &field = fields.at(i);
    if (std::holds_alternative<int>(field)) {
        return type_t::INT;
    }
    if (std::holds_alternative<double>(field)) {
        return type_t::DOUBLE;
    }
    if (std::holds_alternative<std::string>(field)) {
        return type_t::CHAR;
    }
    throw std::logic_error("Unknown field type");
}

size_t Tuple::size() const { return fields.size(); }

const field_t &Tuple::get_field(size_t i) const { return fields.at(i); }

TupleDesc::TupleDesc(const std::vector<type_t> &types, const std::vector<std::string> &names) {
    // TODO pa1
    if (types.size() != names.size()) {
         throw std::invalid_argument("TupleDesc: types and names must have the same length");
        }

        std::unordered_set<std::string> seen;
        for (const auto &n : names) {
            if (!seen.insert(n).second) {
                throw std::logic_error("TupleDesc: duplicate field name: " + n);
            }
        }

        this->types = types;
        this->names = names;
    }

bool TupleDesc::compatible(const Tuple &tuple) const {
    // TODO pa1
    //throw std::runtime_error("not implemented");

    // Check if tuple has the same number of fields as schema
    if (tuple.size() != types.size()) {
            return false;
        }

    // Compare each field type with expected type
    for (size_t i = 0; i < types.size(); i++) {
            if (tuple.field_type(i) != types[i]) {
                return false;
            }
        }

   // If all match
    return true;
    }

size_t TupleDesc::index_of(const std::string &name) const {
    // TODO pa1
    for (size_t i = 0; i < names.size(); i++) {
        if (names[i] == name) {
            return i;
        }
    }
    throw std::invalid_argument("TupleDesc::index_of: field not found: " + name);
}

size_t TupleDesc::offset_of(const size_t &index) const {
    // TODO pa1
    if (index >= types.size()) {
        throw std::out_of_range("TupleDesc::offset_of: index out of range");
    }

    size_t offset = 0;

    for (size_t i = 0; i < index; i++) {
        switch (types[i]) {
        case type_t::INT:
            offset += 4;
            break;
        case type_t::DOUBLE:
            offset += 8;
            break;
        case type_t::CHAR:
            offset += 64;
            break;
        default:
            throw std::logic_error("TupleDesc::offset_of: unknown type");
        }
    }

    return offset;
}

size_t TupleDesc::length() const {
    // TODO pa1
    size_t total = 0;

    for (auto t : types) {
        switch (t) {
        case type_t::INT:
            total += 4;
            break;
        case type_t::DOUBLE:
            total += 8;
            break;
        case type_t::CHAR:
            total += 64;
            break;
        default:
            throw std::logic_error("TupleDesc::length: unknown field type");
        }
    }

    return total;
}

size_t TupleDesc::size() const {
    // TODO pa1
    return types.size();
}

Tuple TupleDesc::deserialize(const uint8_t *data) const
{
    // TODO pa1
    std::vector<field_t> fields;  // will hold all decoded fields
    size_t offset = 0;

    for (auto t : types) {
        switch (t) {
        case type_t::INT: {
                int value;
                std::memcpy(&value, data + offset, sizeof(int));
                fields.push_back(value);
                offset += sizeof(int);
                break;
        }
        case type_t::DOUBLE: {
                double value;
                std::memcpy(&value, data + offset, sizeof(double));
                fields.push_back(value);
                offset += sizeof(double);
                break;
        }
        case type_t::CHAR: {
                char buffer[65];  // one extra for safety / null termination
                std::memcpy(buffer, data + offset, 64);
                buffer[64] = '\0';  // ensure null-termination
                fields.push_back(std::string(buffer));
                offset += 64;
                break;
        }
        default:
            throw std::logic_error("TupleDesc::deserialize: unknown field type");
        }
    }

    return Tuple(fields);
}

void TupleDesc::serialize(uint8_t *data, const Tuple &t) const {
    // TODO pa1
    if (!compatible(t)) {
        throw std::invalid_argument("TupleDesc::serialize: tuple not compatible with schema");
    }

    size_t offset = 0;

    for (size_t i = 0; i < types.size(); i++) {
        const field_t &field = t.get_field(i);

        switch (types[i]) {
        case type_t::INT: {
                int value = std::get<int>(field);
                std::memcpy(data + offset, &value, sizeof(int));
                offset += sizeof(int);
                break;
        }
        case type_t::DOUBLE: {
                double value = std::get<double>(field);
                std::memcpy(data + offset, &value, sizeof(double));
                offset += sizeof(double);
                break;
        }
        case type_t::CHAR: {
                std::string value = std::get<std::string>(field);

                // Ensure exactly 64 bytes (truncate or pad with zeros)
                char buffer[64] = {0};
                std::strncpy(buffer, value.c_str(), 64);

                std::memcpy(data + offset, buffer, 64);
                offset += 64;
                break;
        }
        default:
            throw std::logic_error("TupleDesc::serialize: unknown field type");
        }
    }
}

db::TupleDesc TupleDesc::merge(const TupleDesc &td1, const TupleDesc &td2) {
    // TODO pa1
    std::vector<type_t> mergedTypes = td1.types;
    std::vector<std::string> mergedNames = td1.names;

    // Append fields from td2
    mergedTypes.insert(mergedTypes.end(), td2.types.begin(), td2.types.end());
    mergedNames.insert(mergedNames.end(), td2.names.begin(), td2.names.end());

    return TupleDesc(mergedTypes, mergedNames);
}
