#ifndef GPUIMPL_PTFOPERATOR_HPP
#define GPUIMPL_PTFOPERATOR_HPP
#include "PTFPredicate.hpp"
#include "Operator.hpp"
#include <Util/Util.hpp>
#include <regex>
#include <thrust/host_vector.h>

class Function;
class PTFOperator: public Operator{
public:
    PTFPredicate * predicate;

    PTFOperator(std::string operator_name){
      this->operator_name=operator_name;
    };
    void parseExtended() override;
};

#endif //GPUIMPL_PTFOPERATOR_HPP
