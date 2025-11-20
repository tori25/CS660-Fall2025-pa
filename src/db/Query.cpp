#include <db/Query.hpp>
#include <map>

using namespace db;

void db::projection(const DbFile &in, DbFile &out, const std::vector<std::string> &field_names) {
  // TODO: Implement this function

  const TupleDesc &in_td = in.getTupleDesc();

  for (auto it = in.begin(); it != in.end(); ++it) {
    const Tuple &input_tuple = *it;

    std::vector<field_t> projected;
    projected.reserve(field_names.size());

    for (const std::string &name : field_names) {
      size_t idx = in_td.index_of(name);
      projected.push_back(input_tuple.get_field(idx));
    }

    out.insertTuple(Tuple(projected));
  }
}

void db::filter(const DbFile &in, DbFile &out, const std::vector<FilterPredicate> &pred) {
  // TODO: Implement this function

  const TupleDesc &td = in.getTupleDesc();

  for (auto it = in.begin(); it != in.end(); ++it) {
    const Tuple &t = *it;
    bool ok = true;

    for (const FilterPredicate &p : pred) {
      size_t idx = td.index_of(p.field_name);
      const field_t &v = t.get_field(idx);

      bool cond = false;
      switch (p.op) {
        case PredicateOp::EQ: cond = (v == p.value); break;
        case PredicateOp::NE: cond = (v != p.value); break;
        case PredicateOp::LT: cond = (v <  p.value); break;
        case PredicateOp::LE: cond = (v <= p.value); break;
        case PredicateOp::GT: cond = (v >  p.value); break;
        case PredicateOp::GE: cond = (v >= p.value); break;
      }

      if (!cond) {
        ok = false;
        break;
      }
    }

    if (ok) out.insertTuple(t);
  }
}

void db::aggregate(const DbFile &in, DbFile &out, const Aggregate &agg) {
  // TODO: Implement this function

  const TupleDesc &td = in.getTupleDesc();
  size_t agg_idx = td.index_of(agg.field);


  if (agg.group.has_value()) {
    size_t group_idx = td.index_of(agg.group.value());

    std::map<field_t, std::vector<field_t>> groups;

    for (auto it = in.begin(); it != in.end(); ++it) {
      const Tuple &t = *it;
      groups[t.get_field(group_idx)].push_back(t.get_field(agg_idx));
    }

    for (auto &entry : groups) {
      const field_t &gkey = entry.first;
      const std::vector<field_t> &vals = entry.second;
      if (vals.empty()) continue;

      field_t result;

      if (agg.op == AggregateOp::COUNT) {
        result = static_cast<int>(vals.size());
      }
      else {
        const field_t &first = vals[0];

        if (std::holds_alternative<int>(first)) {
          int acc = 0;
          int mn = std::get<int>(first);
          int mx = std::get<int>(first);

          for (const field_t &v : vals) {
            int x = std::get<int>(v);
            acc += x;
            if (x < mn) mn = x;
            if (x > mx) mx = x;
          }

          switch (agg.op) {
            case AggregateOp::SUM: result = acc; break;
            case AggregateOp::AVG: result = double(acc) / vals.size(); break;
            case AggregateOp::MIN: result = mn; break;
            case AggregateOp::MAX: result = mx; break;
            default: break;
          }
        }
        else {
          double acc = 0;
          double mn = std::get<double>(first);
          double mx = std::get<double>(first);

          for (const field_t &v : vals) {
            double x = std::get<double>(v);
            acc += x;
            if (x < mn) mn = x;
            if (x > mx) mx = x;
          }

          switch (agg.op) {
            case AggregateOp::SUM: result = acc; break;
            case AggregateOp::AVG: result = acc / vals.size(); break;
            case AggregateOp::MIN: result = mn; break;
            case AggregateOp::MAX: result = mx; break;
            default: break;
          }
        }
      }

      // output row: [group_key, result]
      out.insertTuple(Tuple({gkey, result}));
    }
  }


  else {
    std::vector<field_t> vals;

    for (auto it = in.begin(); it != in.end(); ++it) {
      vals.push_back((*it).get_field(agg_idx));
    }

    if (vals.empty()) return;  // produce no output

    field_t result;

    if (agg.op == AggregateOp::COUNT) {
      result = static_cast<int>(vals.size());
    }
    else {
      const field_t &first = vals[0];

      if (std::holds_alternative<int>(first)) {
        int acc = 0;
        int mn = std::get<int>(first);
        int mx = std::get<int>(first);

        for (const field_t &v : vals) {
          int x = std::get<int>(v);
          acc += x;
          if (x < mn) mn = x;
          if (x > mx) mx = x;
        }

        switch (agg.op) {
          case AggregateOp::SUM: result = acc; break;
          case AggregateOp::AVG: result = double(acc) / vals.size(); break;
          case AggregateOp::MIN: result = mn; break;
          case AggregateOp::MAX: result = mx; break;
          default: break;
        }
      }
      else {
        double acc = 0;
        double mn = std::get<double>(first);
        double mx = std::get<double>(first);

        for (const field_t &v : vals) {
          double x = std::get<double>(v);
          acc += x;
          if (x < mn) mn = x;
          if (x > mx) mx = x;
        }

        switch (agg.op) {
          case AggregateOp::SUM: result = acc; break;
          case AggregateOp::AVG: result = acc / vals.size(); break;
          case AggregateOp::MIN: result = mn; break;
          case AggregateOp::MAX: result = mx; break;
          default: break;
        }
      }
    }

    out.insertTuple(Tuple({result}));
  }
}

void db::join(const DbFile &left, const DbFile &right, DbFile &out, const JoinPredicate &pred) {
  // TODO: Implement this function

  const TupleDesc &ltd = left.getTupleDesc();
  const TupleDesc &rtd = right.getTupleDesc();

  size_t li = ltd.index_of(pred.left);
  size_t ri = rtd.index_of(pred.right);

  for (auto itL = left.begin(); itL != left.end(); ++itL) {
    const Tuple &LT = *itL;
    field_t lv = LT.get_field(li);

    for (auto itR = right.begin(); itR != right.end(); ++itR) {
      const Tuple &RT = *itR;
      field_t rv = RT.get_field(ri);

      bool ok = false;
      switch (pred.op) {
        case PredicateOp::EQ: ok = (lv == rv); break;
        case PredicateOp::NE: ok = (lv != rv); break;
        case PredicateOp::LT: ok = (lv <  rv); break;
        case PredicateOp::LE: ok = (lv <= rv); break;
        case PredicateOp::GT: ok = (lv >  rv); break;
        case PredicateOp::GE: ok = (lv >= rv); break;
      }

      if (!ok) continue;

      std::vector<field_t> out_fields;

      // LEFT fields
      for (size_t i = 0; i < LT.size(); ++i)
        out_fields.push_back(LT.get_field(i));

      // RIGHT fields
      if (pred.op == PredicateOp::EQ) {
        for (size_t i = 0; i < RT.size(); ++i)
          if (i != ri) out_fields.push_back(RT.get_field(i));
      } else {
        for (size_t i = 0; i < RT.size(); ++i)
          out_fields.push_back(RT.get_field(i));
      }

      out.insertTuple(Tuple(out_fields));
    }
  }
}