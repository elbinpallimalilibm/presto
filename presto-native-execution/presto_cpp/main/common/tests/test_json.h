/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <fstream>
#include <iosfwd>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>

#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"

namespace fs = boost::filesystem;

namespace nlohmann {

// This is required avoid stack overflow when a gtest error printer is invoked.
// Only happens for us in gcc compiled code.
//
// https://github.com/nlohmann/json/issues/709
//
inline void PrintTo(json const& json, std::ostream* os) {
  *os << json.dump();
}

} // namespace nlohmann

template <typename T>
void testJsonRoundtrip(const json& j, T p) {
  json r = p;
  ASSERT_EQ(j, r) << "conversion back to JSON";

  std::string s = r.dump(); // Dump all the way out to string
  r = json::parse(s); // and back
  ASSERT_EQ(j, r) << "conversion from roundtrip string";
}

inline std::string slurp(const std::string& path) {
  std::ostringstream buf;
  std::ifstream input(path.c_str());
  buf << input.rdbuf();
  return buf.str();
}

inline std::string getDataPath(const std::string& dirUnderFbcode, const std::string& fileName) {
  std::string currentPath = fs::current_path().c_str();
  if (boost::algorithm::ends_with(currentPath, "fbcode")) {
    return currentPath + dirUnderFbcode + fileName;
  }

  // CLion runs the tests from cmake-build-release/ or cmake-build-debug/
  // directory. Hard-coded json files are not copied there and test fails with
  // file not found. Fixing the path so that we can trigger these tests from
  // CLion.
  boost::algorithm::replace_all(currentPath, "cmake-build-release/", "");
  boost::algorithm::replace_all(currentPath, "cmake-build-debug/", "");

  return currentPath + "/data/" + fileName;
}
