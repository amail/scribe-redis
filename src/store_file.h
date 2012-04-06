//  Copyright (c) 2007-2008 Facebook
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
// See accompanying file LICENSE or visit the Scribe site at:
// http://developers.facebook.com/scribe/
//
// @author Bobby Johnson
// @author James Wang
// @author Jason Sobel
// @author Alex Moskalyuk
// @author Avinash Lakshman
// @author Anthony Giardullo
// @author Jan Oravec

#ifndef SCRIBE_STORE_FILE_H
#define SCRIBE_STORE_FILE_H


#include "common.h" // includes std libs, thrift, and stl typedefs
#include "conf.h"
#include "file.h"
#include "conn_pool.h"
#include "store_filebase.h"

/*
 * This file-based store uses an instance of a FileInterface class that
 * handles the details of interfacing with the filesystem. (see file.h)
 */

class FileStore : public FileStoreBase {

 public:
  FileStore(const std::string& category, bool multi_category,
            const std::string& trigger_path, bool is_buffer_file = false);
  ~FileStore();

  boost::shared_ptr<Store> copy(const std::string &category);
  bool handleMessages(boost::shared_ptr<logentry_vector_t> messages);
  bool isOpen();
  void configure(pStoreConf configuration);
  void close();
  void flush();

  // Each read does its own open and close and gets the whole file.
  // This is separate from the write file, and not really a consistent interface.
  bool readOldest(/*out*/ boost::shared_ptr<logentry_vector_t> messages,
                  struct tm* now);
  virtual bool replaceOldest(boost::shared_ptr<logentry_vector_t> messages,
                             struct tm* now);
  void deleteOldest(struct tm* now);
  bool empty(struct tm* now);

 protected:
  // Implement FileStoreBase virtual function
  bool openInternal(bool incrementFilename, struct tm* current_time);
  bool writeMessages(boost::shared_ptr<logentry_vector_t> messages,
                     boost::shared_ptr<FileInterface> write_file =
                     boost::shared_ptr<FileInterface>());

  bool isBufferFile;
  bool addNewlines;

  // State
  boost::shared_ptr<FileInterface> writeFile;

 private:
  // disallow copy, assignment, and empty construction
  FileStore(FileStore& rhs);
  FileStore& operator=(FileStore& rhs);
};

#endif // SCRIBE_STORE_FILE_H

