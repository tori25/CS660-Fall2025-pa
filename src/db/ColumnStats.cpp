#include <db/ColumnStats.hpp>
#include <cmath>

using namespace db;

ColumnStats::ColumnStats(unsigned buckets, int min, int max)
    : min_(min), max_(max), num_buckets_(buckets), total_count_(0) {
  // Initialize bucket_width_ (not used but kept for compatibility)
  bucket_width_ = (max - min + 1) / static_cast<int>(buckets);
  
  // Initialize all buckets to 0  
  buckets_.resize(buckets, 0);
}

void ColumnStats::addValue(int v) {
  // Only add values within the valid range
  if (v < min_ || v > max_) {
    return;
  }

  // Calculate actual bucket width as double 
  double bucket_width = static_cast<double>(max_ - min_ + 1) / num_buckets_;
  
  // Calculate which bucket this value belongs to
  int bucket_index = static_cast<int>((v - min_) / bucket_width);

  // Handle edge case where v == max_
  if (bucket_index >= static_cast<int>(num_buckets_)) {
    bucket_index = num_buckets_ - 1;
  }

  // Increment the bucket count
  buckets_[bucket_index]++;
  total_count_++;
}

size_t ColumnStats::estimateCardinality(PredicateOp op, int v) const {
  // If histogram is empty, return 0
  if (total_count_ == 0) {
    return 0;
  }

  // Calculate actual bucket width as double for fractional calculations
  double bucket_width = static_cast<double>(max_ - min_ + 1) / num_buckets_;

  // Handle values outside the range
  if (v < min_) {
    switch (op) {
      case PredicateOp::LT:
      case PredicateOp::LE:
        return 0;
      case PredicateOp::GT:
      case PredicateOp::GE:
      case PredicateOp::NE:
        return total_count_;
      case PredicateOp::EQ:
        return 0;
    }
  }

  if (v > max_) {
    switch (op) {
      case PredicateOp::LT:
      case PredicateOp::LE:
      case PredicateOp::NE:
        return total_count_;
      case PredicateOp::GT:
      case PredicateOp::GE:
      case PredicateOp::EQ:
        return 0;
    }
  }

  // Find which bucket v falls into
  int bucket_index = static_cast<int>((v - min_) / bucket_width);

  // Handle edge case where v == max_
  if (bucket_index >= static_cast<int>(num_buckets_)) {
    bucket_index = num_buckets_ - 1;
  }

  size_t result = 0;

  switch (op) {
    case PredicateOp::EQ: {
      // For equality: h / w
      size_t h = buckets_[bucket_index];
      double w = std::max(bucket_width, 1.0); // width is at least 1
      result = static_cast<size_t>(h / w);
      break;
    }

    case PredicateOp::NE: {
      // Not equal: total_count - (estimate for EQ)
      size_t eq_estimate = estimateCardinality(PredicateOp::EQ, v);
      result = total_count_ - eq_estimate;
      break;
    }

    case PredicateOp::GT: {
      // Greater than: fraction of current bucket + all buckets to the right
      size_t h = buckets_[bucket_index];

      // Calculate the right endpoint of the bucket
      double bucket_right = min_ + (bucket_index + 1) * bucket_width;

      // Fraction of current bucket that satisfies v > c
      double fraction = (bucket_right - v - 1) / bucket_width;
      result = static_cast<size_t>(h * fraction);

      // Add all buckets to the right
      for (int i = bucket_index + 1; i < static_cast<int>(num_buckets_); i++) {
        result += buckets_[i];
      }
      break;
    }

    case PredicateOp::GE: {
      // Greater than or equal: similar to GT but include the value
      size_t h = buckets_[bucket_index];

      // Calculate the right endpoint of the bucket
      double bucket_right = min_ + (bucket_index + 1) * bucket_width;

      // Fraction of current bucket that satisfies v >= c
      double fraction = (bucket_right - v) / bucket_width;
      result = static_cast<size_t>(h * fraction);

      // Add all buckets to the right
      for (int i = bucket_index + 1; i < static_cast<int>(num_buckets_); i++) {
        result += buckets_[i];
      }
      break;
    }

    case PredicateOp::LT: {
      // Less than: all buckets to the left + fraction of current bucket
      size_t h = buckets_[bucket_index];

      // Calculate the left endpoint of the bucket
      double bucket_left = min_ + bucket_index * bucket_width;

      // Fraction of current bucket that satisfies v < c
      double fraction = (v - bucket_left) / bucket_width;
      result = static_cast<size_t>(h * fraction);

      // Add all buckets to the left
      for (int i = 0; i < bucket_index; i++) {
        result += buckets_[i];
      }
      break;
    }

    case PredicateOp::LE: {
      // Less than or equal: LT(v) + EQ(v)
      result = estimateCardinality(PredicateOp::LT, v) + estimateCardinality(PredicateOp::EQ, v);
      break;
    }
  }

  return result;
}
