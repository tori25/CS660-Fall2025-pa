#pragma once

#include <db/Query.hpp>
#include <vector>

namespace db {

  class ColumnStats {
  private:
    std::vector<size_t> buckets_;   // Histogram buckets
    int min_;
    int max_;
    unsigned num_buckets_;
    int bucket_width_;              // Width of each bucket (MUST BE INT)
    size_t total_count_;            // Total values added

  public:
    ColumnStats(unsigned buckets, int min, int max);

    void addValue(int v);

    size_t estimateCardinality(PredicateOp op, int v) const;
  };

}