#ifndef UTIL_HPP
#define UTIL_HPP

#include <iostream>
#include <regex>
#include <sstream>
#include <string>
#include <vector>
#include <cstdint>
#include <chrono>

inline int64_t get_nano_timestamp() {
  return std::chrono::high_resolution_clock::now().time_since_epoch().count() / 1000000;
}

inline void print_pcie_starting_timestamp() {
  std::cout << "Profiling: GHive 'PCIe' starting at time " << get_nano_timestamp() << " ms" << std::endl;
}

inline void print_pcie_ending_timestamp() {
  std::cout << "Profiling: GHive 'PCIe' endinging at time " << get_nano_timestamp() << " ms" << std::endl;
}

inline void print_gpu_processing_starting_timestamp_for_operator_name(const std::string& name) {
  std::cout << "Profiling: GHive 'GPU Processing' for operator " << name << " starting at time " << get_nano_timestamp() << " ms" << std::endl;
}

inline void print_gpu_processing_ending_timestamp_for_operator_name(const std::string& name) {
  std::cout << "Profiling: GHive 'GPU Processing' for operator " << name << " ending at time " << get_nano_timestamp() << " ms" << std::endl;
}

inline void print_jni_ending_timestamp() {
  std::cout << "Profiling: GHive 'JNI' ending at time " << get_nano_timestamp() << " ns" << std::endl;
}

inline void split(std::vector<std::string>& output, const std::string& input,
                  const std::string& delimiter) {
  uint32_t left = 0;
  uint32_t right = input.find(delimiter);
  output.push_back(input.substr(left, right));
  while (input.find(delimiter, right + delimiter.size()) != std::string::npos) {
    left = right;
    right = input.find(delimiter, right + delimiter.size());
    output.push_back(
        input.substr(left + delimiter.size(), right - left - delimiter.size()));
  }
  if (right + delimiter.size() < input.size()) {
    output.push_back(input.substr(right + delimiter.size(),
                                  input.size() - right - delimiter.size()));
  }
}

inline void split(const std::string& s, std::vector<std::string>& tokens,
                  const std::string& delimiters) {
  if (delimiters.size() > 1) {
    split(tokens, s, delimiters);
    return;
  }
  std::string::size_type lastPos = s.find_first_not_of(delimiters, 0);
  std::string::size_type pos = s.find_first_of(delimiters, lastPos);
  while (std::string::npos != pos || std::string::npos != lastPos) {
    tokens.push_back(s.substr(lastPos, pos - lastPos));
    lastPos = s.find_first_not_of(delimiters, pos);
    pos = s.find_first_of(delimiters, lastPos);
  }
}

inline void split_unquote(const std::string& s,
                          std::vector<std::string>& tokens,
                          const std::string& delimiters = " ") {
  std::string::size_type lastPos = s.find_first_not_of(delimiters, 0);
  std::string::size_type pos = s.find_first_of(delimiters, lastPos);
  while (std::string::npos != pos || std::string::npos != lastPos) {
    std::string token = s.substr(lastPos, pos - lastPos);
    std::smatch content;
    if (std::regex_search(token, content, std::regex("\"(.*)\""))) {
      tokens.push_back(content[1]);
    } else {
      tokens.push_back(s.substr(lastPos, pos - lastPos));  // unquote
    }
    lastPos = s.find_first_not_of(delimiters, pos);
    pos = s.find_first_of(delimiters, lastPos);
  }
}

inline void print_vector(std::vector<std::string>& v,
                         std::string name = "vector") {
  for (int i = 0; i < v.size(); i++) {
    std::cout << name << "[" << i << "]:" + v[i] + "; ";
  }
  std::cout << std::endl;
}

inline std::string vector_to_string(std::vector<std::string>& v,
                                    std::string name = "") {
  std::stringstream ss;
  //assert(v.size() != 0);
  if (name == "") {
    for (std::string content: v) {
      ss << content << ";";
    }
  }
  else {
    for (uint32_t i = 0; i < v.size(); i++) {
      ss << name << "[" << i << "]:" << v[i] << "; ";
    }
  }
  //std::cout << "FILE: " << __FILE__ << "; LINE: " << __LINE__ << std::endl;
  std::string ret;
  ss >> ret;
  return ret;
}

template <typename T>
inline std::string vector_to_string(std::vector<T>& v,
                                    std::string name = "") {
  std::stringstream ss;
  //assert(v.size() != 0);
  if (name == "") {
    for (auto content: v) {
      ss << content << ";";
    }
  }
  else {
    for (uint32_t i = 0; i < v.size(); i++) {
      ss << name << "[" << i << "]:" << v[i] << "; ";
    }
  }
  //std::cout << "FILE: " << __FILE__ << "; LINE: " << __LINE__ << std::endl;
  std::string ret;
  ss >> ret;
  return ret;
}
// https://stackoverflow.com/questions/216823/whats-the-best-way-to-trim-stdstring
/**
 * Trim from start in place.
 * @param str The string to trim.
 */
inline void ltrim(std::string& str) {
  str.erase(str.begin(),
            std::find_if(str.begin(), str.end(),
                         [](unsigned char ch) { return !std::isspace(ch); }));
}

/**
 * Trim from end in place.
 * @param str The string to trim.
 */
inline void rtrim(std::string& str) {
  str.erase(std::find_if(str.rbegin(), str.rend(),
                         [](unsigned char ch) { return !std::isspace(ch); })
                .base(),
            str.end());
}

/**
 * Trim a string from start and end.
 * @param str The string to trim.
 * @return The trimmed string.
 */
inline std::string trim(std::string& str) {
  std::string res = str;
  ltrim(res);
  rtrim(res);
  return res;
}

inline int *vector_to_ptr(std::vector<uint32_t> vec) {
  int*ret = new int[vec.size()];
  for (uint32_t i = 0; i < vec.size(); i ++) {
    ret[i] = vec[i];
  }
  return ret;
}

template<typename ITER>
void init_index_sequence(ITER begin, ITER end) {
    for (uint64_t i = 0; begin != end; ++begin, ++i) {
        *begin = (decltype(*begin)) i;
    }
}

#endif