#pragma once

#include <map>
#include <cstdint>

template<class T, class P>
class Tree;

//used to define the interface of all benchmarking trees
// the param `epoch` is uesd in apex
template <class T, class P>
class Tree {
 public:
  typedef std::pair<T, P> V;
  virtual void bulk_load(const V[], int) = 0;
  virtual bool insert(const T&, const P&) = 0;
  virtual bool search(const T&, P&) = 0;
  virtual bool erase(const T&, uint64_t *log_offset = nullptr) = 0;
  virtual bool update(const T&, const P&, uint64_t *log_offset = nullptr) = 0;
  // Return #keys really scanned
  virtual int range_scan_by_size(const T&, uint32_t, V*& resul) = 0;

  virtual void get_info() = 0;
};