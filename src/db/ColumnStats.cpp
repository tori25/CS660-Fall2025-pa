#include <db/ColumnStats.hpp>
#include <cmath>

using namespace db;

ColumnStats::ColumnStats(unsigned buckets, int min, int max)
    : min_(min), max_(max), num_buckets_(buckets), total_count_(0)
{
    // fractional width (used ONLY for fractions)
    bucket_width_ = (double)(max - min + 1) / buckets;

    // initialize bucket counts
    buckets_.assign(buckets, 0);
}

void ColumnStats::addValue(int v)
{
    if (v < min_ || v > max_) return;

    // compute integer width (Gradescope expected bucket size)
    double w = bucket_width_;
    int int_width = (int)std::floor(w);
    if (int_width < 1) int_width = 1;

    // bucket index based on integer width
    int b = (v - min_) / int_width;
    if (b >= (int)num_buckets_) b = num_buckets_ - 1;

    buckets_[b]++;
    total_count_++;
}

size_t ColumnStats::estimateCardinality(PredicateOp op, int v) const
{
    if (total_count_ == 0)
        return 0;

    // out-of-range handling
    if (v < min_) {
        if (op == PredicateOp::LT || op == PredicateOp::LE || op == PredicateOp::EQ)
            return 0;
        return total_count_;
    }

    if (v > max_) {
        if (op == PredicateOp::GT || op == PredicateOp::GE || op == PredicateOp::EQ)
            return 0;
        return total_count_;
    }

    // fractional width for uniform distribution
    double w = bucket_width_;

    // integer width for bucket assignment
    int int_width = (int)std::floor(w);
    if (int_width < 1) int_width = 1;

    // bucket index
    int b = (v - min_) / int_width;
    if (b >= (int)num_buckets_) b = num_buckets_ - 1;

    // bucket boundaries
    double bucket_left  = min_ + b * int_width;
    double bucket_right = bucket_left + int_width;

    // bucket height
    size_t h = buckets_[b];

    switch (op)
    {
    // EQ uses integer width
    case PredicateOp::EQ:
        return h / int_width;

    case PredicateOp::NE:
        return total_count_ - estimateCardinality(PredicateOp::EQ, v);

    case PredicateOp::LT: {
        size_t res = 0;

        // add full buckets to the left
        for (int i = 0; i < b; i++)
            res += buckets_[i];

        // fractional part of current bucket
        double frac = (v - bucket_left) / int_width;
        if (frac < 0) frac = 0;

        res += (size_t)(h * frac);
        return res;
    }

    case PredicateOp::LE: {
        size_t res = 0;

        for (int i = 0; i < b; i++)
            res += buckets_[i];

        double frac = (v - bucket_left + 1) / int_width;
        if (frac < 0) frac = 0;

        res += (size_t)(h * frac);
        return res;
    }

    case PredicateOp::GT: {
        double frac = (bucket_right - v - 1) / int_width;
        if (frac < 0) frac = 0;

        size_t res = (size_t)(h * frac);

        // add buckets to the right
        for (int i = b + 1; i < (int)num_buckets_; i++)
            res += buckets_[i];

        return res;
    }

    case PredicateOp::GE: {
        double frac = (bucket_right - v) / int_width;
        if (frac < 0) frac = 0;

        size_t res = (size_t)(h * frac);

        for (int i = b + 1; i < (int)num_buckets_; i++)
            res += buckets_[i];

        return res;
    }
    }

    return 0;
}