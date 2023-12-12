#include "check_result.hpp"
#include "kml_join.hpp"
#include <map>
#include <iostream>

int check_result(input_tuple_t *relR, input_tuple_t *relS, output_tuple_t *relRS, int numr, int nums, int numrs)
{
  // Verify the result
  // Map of key -> rid1 in first table
  std::map<int, int> key_map;
  // Map of rid2 -> output tuple (since rid2 is unique and only matches one tuple in the first table)
  std::map<int, std::pair<output_tuple_t, bool>> rid_map;
  for (int i = 0; i < numr; i++)
  {
    auto key = relR[i].key;
    auto rid = relR[i].rid;
    key_map[key] = rid;
  }

  for (int i = 0; i < nums; i++)
  {
    auto key = relS[i].key;
    auto rid2 = relS[i].rid;
    if (key_map.find(key) != key_map.end())
    {
      auto rid1 = key_map[key];
      output_tuple_t output_tuple;
      output_tuple.rid1 = rid1;
      output_tuple.rid2 = rid2;
      output_tuple.key = key;
      rid_map[rid2] = std::make_pair(output_tuple, false);
    }
  }

  for (int i = 0; i < numrs; i++)
  {
    auto rid2 = relRS[i].rid2;
    if (rid_map.find(rid2) != rid_map.end())
    {
      auto output_tuple = rid_map[rid2].first;
      if (output_tuple.rid1 != relRS[i].rid1)
      {
        std::cout << "rid1 mismatch: " << output_tuple.rid1 << " " << relRS[i].rid1 << std::endl;
        return EXIT_FAILURE;
      }
      if (output_tuple.rid2 != relRS[i].rid2)
      {
        std::cout << "rid2 mismatch: " << output_tuple.rid2 << " " << relRS[i].rid2 << std::endl;
        return EXIT_FAILURE;
      }
      if (output_tuple.key != relRS[i].key)
      {
        std::cout << "key mismatch: " << output_tuple.key << " " << relRS[i].key << std::endl;
        return EXIT_FAILURE;
      }
      if (rid_map[rid2].second == false)
      {
        rid_map[rid2].second = true;
      }
      else
      {
        std::cout << "duplicate rid2: " << rid2 << std::endl;
        return EXIT_FAILURE;
      }
    }
    else
    {
      std::cout << "rid2 not found: " << rid2 << std::endl;
      return EXIT_FAILURE;
    }
  }

  for (auto it = rid_map.begin(); it != rid_map.end(); it++)
  {
    if (it->second.second == false)
    {
      std::cout << "tuple with missing with rid2: " << it->first << std::endl;
      return EXIT_FAILURE;
    }
  }

  return EXIT_SUCCESS;
}