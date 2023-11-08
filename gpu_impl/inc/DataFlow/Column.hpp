#ifndef Column_HPP
#define Column_HPP
#include <cstdint>

typedef enum ColumnType : int {
  LONG = 0,
  DOUBLE = 1,
  INT = 2,
  STRING = 3,
  DEPEND = -1
} ColumnType;

typedef struct Column {
 public:
  void *data_ptr;
  void *data_ptr_aux;
  ColumnType type;
  uint32_t row_num;
  uint32_t char_size; // only used for string.

  Column(ColumnType _type, uint32_t _row_num);

  Column(ColumnType _type);
  Column() = default;

  Column(ColumnType _type, uint32_t _row_num, void *_data_ptr);

  // Column(ColumnType _type, uint32_t _row_num, void *_data_ptr, void *_data_ptr_aux);
  Column(ColumnType _type, uint32_t _row_num, void *_data_ptr, void *_data_ptr_aux, uint32_t _char_size);

  void set_data_ptr(void *_data_ptr);

  void set_data_ptr(void *_data_ptr, void *_data_ptr_aux, int32_t char_size);

} Column;

#endif