#include "DataFlow/Column.hpp"

Column::Column(ColumnType _type, uint32_t _row_num) :
    type(_type), row_num(_row_num), data_ptr(nullptr), data_ptr_aux(nullptr), char_size(0) {}

Column::Column(ColumnType _type) :
    type(_type), row_num(0), data_ptr(nullptr), data_ptr_aux(nullptr), char_size(0) {}

void Column::set_data_ptr(void *_data_ptr) {
  data_ptr = _data_ptr;
}

// todo: delete dangerous method
void Column::set_data_ptr(void *_data_ptr, void *_data_ptr_aux, int32_t _char_size) {
  data_ptr = _data_ptr;
  data_ptr_aux = _data_ptr_aux;
  // char_size = ((int32_t *) data_ptr_aux)[2 * row_num - 1];
  this->char_size = _char_size;
}

Column::Column(ColumnType _type, uint32_t _row_num, void *_data_ptr)
    : type(_type), row_num(_row_num), data_ptr(_data_ptr), data_ptr_aux(nullptr), char_size(0) {}

// Column::Column(ColumnType _type, uint32_t _row_num, void *_data_ptr, void *_data_ptr_aux) :
//     type(_type),
//     row_num(_row_num),
//     data_ptr(_data_ptr),
//     data_ptr_aux(_data_ptr_aux),
//     char_size(((int32_t *) data_ptr_aux)[2 * row_num - 1]) {}

Column::Column(ColumnType _type, uint32_t _row_num, void *_data_ptr, void *_data_ptr_aux, uint32_t _char_size) :
    type(_type),
    row_num(_row_num),
    data_ptr(_data_ptr),
    data_ptr_aux(_data_ptr_aux),
    char_size(_char_size) {}
