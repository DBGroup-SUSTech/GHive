#ifndef AGGREGATIONDESC_HPP
#define AGGREGATIONDESC_HPP

#include <cstdint>

enum AggregationType {
  //  Sum.
  SUM = 0,

  //  Max.
  MAX = 1,

  //  Min.
  MIN = 2,

  //  Average.
  AVG = 3,

  //  Count. Does this kind of aggregation really exist?
  CNT = 4,

  RANK = 5,

  UNKNOWN = 6,
};

class AggregationDesc {
 private:
 public:
  AggregationType type;
  uint32_t index;
  AggregationDesc(AggregationType type, uint32_t index) {
    this->type = type;
    this->index = index;
  }
};

#endif