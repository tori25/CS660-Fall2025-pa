#pragma once

#include <db/Query.hpp>
#include <vector>

namespace db {

  class ColumnStats {
  private:
    std::vector<size_t> buckets_;   // histogram buckets
    int min_;                       // minimum value
    int max_;                       // maximum value
    unsigned num_buckets_;          // number of buckets
    int bucket_width_;              // INTEGER bucket width
    size_t total_count_;            // total values added

  public:
    ColumnStats(unsigned buckets, int min, int max);

    void addValue(int v);

    size_t estimateCardinality(PredicateOp op, int v) const;
  };

} // namespace db