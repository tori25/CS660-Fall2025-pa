#include <db/Query.hpp>

using namespace db;

void db::projection(const DbFile &in, DbFile &out, const std::vector<std::string> &field_names) {
  // TODO: Implement this function
  const TupleDesc &td = in.getTupleDesc();
  std::vector<size_t> indices;
  for (const std::string &field_name : field_names) {
    indices.push_back(td.index_of(field_name));
  }
  for (const Tuple &t : in) {
    std::vector<field_t> fields;
    for (size_t i : indices) {
      fields.push_back(t.get_field(i));
    }
    Tuple new_tuple(fields);
    out.insertTuple(new_tuple);
  }
}

bool eval(const field_t &f1, const field_t &f2, PredicateOp op) {
  switch (op) {
  case PredicateOp::EQ:
    return f1 == f2;
  case PredicateOp::NE:
    return f1 != f2;
  case PredicateOp::LT:
    return f1 < f2;
  case PredicateOp::LE:
    return f1 <= f2;
  case PredicateOp::GT:
    return f1 > f2;
  case PredicateOp::GE:
    return f1 >= f2;
  }
  return false;
}

void db::filter(const DbFile &in, DbFile &out, const std::vector<FilterPredicate> &pred) {
  // TODO: Implement this function
  for (const Tuple &t : in) {
    bool keep = true;
    for (const FilterPredicate &p : pred) {
      size_t field = in.getTupleDesc().index_of(p.field_name);
      keep = eval(t.get_field(field), p.value, p.op);
      if (!keep) {
        break;
      }
    }
    if (keep) {
      out.insertTuple(t);
    }
  }
}

void db::aggregate(const DbFile &in, DbFile &out, const Aggregate &agg) {
  // TODO: Implement this function
  std::unordered_map<field_t, int> values;
  std::unordered_map<field_t, int> counts;
  const TupleDesc &td = in.getTupleDesc();
  size_t group_index = agg.group.has_value() ? td.index_of(agg.group.value()) : -1;
  size_t field_index = td.index_of(agg.field);
  for (const Tuple &t : in) {
    const field_t &key = agg.group.has_value() ? t.get_field(group_index) : "";
    const field_t &f = t.get_field(field_index);
    int value = std::get<int>(f);
    if (values.find(key) == values.end()) {
      counts[key] = 1;
      values[key] = value;
    } else if (agg.op == AggregateOp::COUNT) {
      counts[key]++;
    } else if (agg.op == AggregateOp::SUM) {
      values[key] += value;
    } else if (agg.op == AggregateOp::AVG) {
      values[key] += value;
      counts[key]++;
    } else if (agg.op == AggregateOp::MIN) {
      values[key] = std::min(values[key], value);
    } else if (agg.op == AggregateOp::MAX) {
      values[key] = std::max(values[key], value);
    }
  }
  for (const auto &[key, value] : values) {
    std::vector<field_t> fields;
    if (agg.group.has_value()) {
      fields.push_back(key);
    }
    if (agg.op == AggregateOp::AVG) {
      fields.emplace_back(double(value) / counts[key]);
    } else if (agg.op == AggregateOp::COUNT) {
      fields.emplace_back(counts[key]);
    } else {
      fields.emplace_back(value);
    }
    Tuple new_tuple(fields);
    out.insertTuple(new_tuple);
  }
}

void db::join(const DbFile &left, const DbFile &right, DbFile &out, const JoinPredicate &pred) {
  // TODO: Implement this function
  const TupleDesc &left_td = left.getTupleDesc();
  const TupleDesc &right_td = right.getTupleDesc();
  size_t left_index = left_td.index_of(pred.left);
  size_t right_index = right_td.index_of(pred.right);
  for (auto it1 = left.begin(); it1 != left.end(); ++it1) {
    for (auto it2 = right.begin(); it2 != right.end(); ++it2) {
      const auto &left_t = left.getTuple(it1);
      const auto &right_t = right.getTuple(it2);
      if (eval(left_t.get_field(left_index), right_t.get_field(right_index), pred.op)) {
        std::vector<field_t> fields;
        for (size_t i = 0; i < left_td.size(); i++) {
          fields.push_back(left_t.get_field(i));
        }
        for (size_t i = 0; i < right_td.size(); i++) {
          if (pred.op != PredicateOp::EQ || i != right_index) {
            fields.push_back(right_t.get_field(i));
          }
        }
        Tuple new_tuple(fields);
        out.insertTuple(new_tuple);
      }
    }
  }
}
