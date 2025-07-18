/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

/// \file iceberg/manifest_reader.h
/// Data reader interface for manifest files.

#include <memory>
#include <span>

#include "iceberg/file_reader.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Read manifest entries from a manifest file.
class ICEBERG_EXPORT ManifestReader {
 public:
  virtual ~ManifestReader() = default;
  virtual Result<std::span<std::unique_ptr<ManifestEntry>>> Entries() const = 0;

 private:
  std::unique_ptr<Reader> reader_;
};

/// \brief Read manifest files from a manifest list file.
class ICEBERG_EXPORT ManifestListReader {
 public:
  virtual ~ManifestListReader() = default;
  virtual Result<std::span<std::unique_ptr<ManifestFile>>> Files() const = 0;

 private:
  std::unique_ptr<Reader> reader_;
};

/// \brief Creates a reader for the manifest list.
/// \param file_path Path to the manifest list file.
/// \return A Result containing the reader or an error.
Result<std::unique_ptr<ManifestListReader>> CreateManifestListReader(
    std::string_view file_path) {
  return NotImplemented("CreateManifestListReader is not implemented yet.");
}

/// \brief Creates a reader for a manifest file.
/// \param file_path Path to the manifest file.
/// \return A Result containing the reader or an error.
Result<std::unique_ptr<ManifestReader>> CreateManifestReader(std::string_view file_path) {
  return NotImplemented("CreateManifestReader is not implemented yet.");
}

}  // namespace iceberg
