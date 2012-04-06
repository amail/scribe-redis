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

#ifndef SCRIBE_STORE_FILEBASE_H
#define SCRIBE_STORE_FILEBASE_H


#include "common.h" // includes std libs, thrift, and stl typedefs
#include "conf.h"
#include "file.h"
#include "conn_pool.h"

/* defines used by the store class */
enum roll_period_t {
  ROLL_NEVER,
  ROLL_HOURLY,
  ROLL_DAILY,
  ROLL_OTHER
};

/*
 * Abstract class that serves as a base for file-based stores.
 * This class has logic for naming files and deciding when to rotate.
 */
class FileStoreBase : public Store {
 public:
  FileStoreBase(const std::string& category, const std::string &type,
                bool multi_category, const std::string& trigger_path);
  ~FileStoreBase();

  virtual void copyCommon(const FileStoreBase *base);
  bool open();
  void configure(pStoreConf configuration);
  void periodicCheck();

 protected:
  // We need to pass arguments to open when called internally.
  // The external open function just calls this with default args.
  virtual bool openInternal(bool incrementFilename, struct tm* current_time) = 0;
  virtual void rotateFile(time_t currentTime = 0);


  // appends information about the current file to a log file in the same directory
  virtual void printStats();

  // Returns the number of bytes to pad to align to the specified block size
  unsigned long bytesToPad(unsigned long next_message_length,
                           unsigned long current_file_size,
                           unsigned long chunk_size);

  // A full filename includes an absolute path and a sequence number suffix.
  std::string makeBaseFilename(struct tm* creation_time);
  std::string makeFullFilename(int suffix, struct tm* creation_time,
                               bool use_full_path = true);
  std::string makeBaseSymlink();
  std::string makeFullSymlink();
  int  findOldestFile(const std::string& base_filename);
  int  findNewestFile(const std::string& base_filename);
  int  getFileSuffix(const std::string& filename,
                     const std::string& base_filename);
  void setHostNameSubDir();

  // Configuration
  std::string baseFilePath;
  std::string subDirectory;
  std::string filePath;
  std::string baseFileName;
  std::string baseSymlinkName;
  unsigned long maxSize;
  unsigned long maxWriteSize;
  roll_period_t rollPeriod;
  time_t rollPeriodLength;
  unsigned long rollHour;
  unsigned long rollMinute;
  std::string fsType;
  unsigned long chunkSize;
  bool writeMeta;
  bool writeCategory;
  bool createSymlink;
  bool writeStats;

  // State
  unsigned long currentSize;
  time_t lastRollTime;         // either hour, day or time since epoch,
                               // depending on rollPeriod
  std::string currentFilename; // this isn't used to choose the next file name,
                               // we just need it for reporting
  unsigned long eventsWritten; // This is how many events this process has
                               // written to the currently open file. It is NOT
                               // necessarily the number of lines in the file

 private:
  // disallow copy, assignment, and empty construction
  FileStoreBase(FileStoreBase& rhs);
  FileStoreBase& operator=(FileStoreBase& rhs);
};

#endif // SCRIBE_STORE_FILEBASE_H

