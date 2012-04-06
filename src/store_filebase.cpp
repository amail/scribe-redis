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

#include "common.h"
#include "store.h"
#include "store_filebase.h"

using namespace std;
using namespace boost;
using namespace boost::filesystem;

#define DEFAULT_FILESTORE_MAX_SIZE               1000000000
#define DEFAULT_FILESTORE_MAX_WRITE_SIZE         1000000
#define DEFAULT_FILESTORE_ROLL_HOUR              1
#define DEFAULT_FILESTORE_ROLL_MINUTE            15

FileStoreBase::FileStoreBase(const string& category, const string &type,
                             bool multi_category, const string& trigger_path)
  : Store(category, type, multi_category, trigger_path),
    baseFilePath("/tmp"),
    subDirectory(""),
    filePath("/tmp"),
    baseFileName(category),
    baseSymlinkName(""),
    maxSize(DEFAULT_FILESTORE_MAX_SIZE),
    maxWriteSize(DEFAULT_FILESTORE_MAX_WRITE_SIZE),
    rollPeriod(ROLL_NEVER),
    rollPeriodLength(0),
    rollHour(DEFAULT_FILESTORE_ROLL_HOUR),
    rollMinute(DEFAULT_FILESTORE_ROLL_MINUTE),
    fsType("std"),
    chunkSize(0),
    writeMeta(false),
    writeCategory(false),
    createSymlink(true),
    writeStats(false),
    currentSize(0),
    lastRollTime(0),
    eventsWritten(0) {
}

FileStoreBase::~FileStoreBase() {
}

void FileStoreBase::configure(pStoreConf configuration) {

  // We can run using defaults for all of these, but there are
  // a couple of suspicious things we warn about.
  std::string tmp;
  configuration->getString("file_path", baseFilePath);
  configuration->getString("sub_directory", subDirectory);
  configuration->getString("use_hostname_sub_directory", tmp);

  if (0 == tmp.compare("yes")) {
    setHostNameSubDir();
  }

  filePath = baseFilePath;
  if (!subDirectory.empty()) {
    filePath += "/" + subDirectory;
  }


  if (!configuration->getString("base_filename", baseFileName)) {
    LOG_OPER("[%s] WARNING: Bad config - no base_filename specified for file store", categoryHandled.c_str());
  }

  // check if symlink name is optionally specified
  configuration->getString("base_symlink_name", baseSymlinkName);

  if (configuration->getString("rotate_period", tmp)) {
    if (0 == tmp.compare("hourly")) {
      rollPeriod = ROLL_HOURLY;
    } else if (0 == tmp.compare("daily")) {
      rollPeriod = ROLL_DAILY;
    } else if (0 == tmp.compare("never")) {
      rollPeriod = ROLL_NEVER;
    } else {
      errno = 0;
      char* endptr;
      rollPeriod = ROLL_OTHER;
      rollPeriodLength = strtol(tmp.c_str(), &endptr, 10);

      bool ok = errno == 0 && rollPeriodLength > 0 && endptr != tmp.c_str() &&
                (*endptr == '\0' || endptr[1] == '\0');
      switch (*endptr) {
        case 'w':
          rollPeriodLength *= 60 * 60 * 24 * 7;
          break;
        case 'd':
          rollPeriodLength *= 60 * 60 * 24;
          break;
        case 'h':
          rollPeriodLength *= 60 * 60;
          break;
        case 'm':
          rollPeriodLength *= 60;
          break;
        case 's':
        case '\0':
          break;
        default:
          ok = false;
          break;
      }

      if (!ok) {
        rollPeriod = ROLL_NEVER;
        LOG_OPER("[%s] WARNING: Bad config - invalid format of rotate_period,"
                 " rotations disabled", categoryHandled.c_str());
      }
    }
  }

  if (configuration->getString("write_meta", tmp)) {
    if (0 == tmp.compare("yes")) {
      writeMeta = true;
    }
  }
  if (configuration->getString("write_category", tmp)) {
    if (0 == tmp.compare("yes")) {
      writeCategory = true;
    }
  }

  if (configuration->getString("create_symlink", tmp)) {
    if (0 == tmp.compare("yes")) {
      createSymlink = true;
    } else {
      createSymlink = false;
    }
  }

  if (configuration->getString("write_stats", tmp)) {
    if (0 == tmp.compare("yes")) {
      writeStats = true;
    } else {
      writeStats = false;
    }
  }

  configuration->getString("fs_type", fsType);

  configuration->getUnsigned("max_size", maxSize);
  configuration->getUnsigned("max_write_size", maxWriteSize);
  configuration->getUnsigned("rotate_hour", rollHour);
  configuration->getUnsigned("rotate_minute", rollMinute);
  configuration->getUnsigned("chunk_size", chunkSize);
}

void FileStoreBase::copyCommon(const FileStoreBase *base) {
  subDirectory = base->subDirectory;
  chunkSize = base->chunkSize;
  maxSize = base->maxSize;
  maxWriteSize = base->maxWriteSize;
  rollPeriod = base->rollPeriod;
  rollPeriodLength = base->rollPeriodLength;
  rollHour = base->rollHour;
  rollMinute = base->rollMinute;
  fsType = base->fsType;
  writeMeta = base->writeMeta;
  writeCategory = base->writeCategory;
  createSymlink = base->createSymlink;
  baseSymlinkName = base->baseSymlinkName;
  writeStats = base->writeStats;

  /*
   * append the category name to the base file path and change the
   * baseFileName to the category name. these are arbitrary, could be anything
   * unique
   */
  baseFilePath = base->baseFilePath + std::string("/") + categoryHandled;
  filePath = baseFilePath;
  if (!subDirectory.empty()) {
    filePath += "/" + subDirectory;
  }

  baseFileName = categoryHandled;
}

bool FileStoreBase::open() {
  return openInternal(false, NULL);
}

void FileStoreBase::periodicCheck() {

  time_t rawtime = time(NULL);
  struct tm timeinfo;
  localtime_r(&rawtime, &timeinfo);

  // Roll the file if we're over max size, or an hour or day has passed
  bool rotate = ((currentSize > maxSize) && (maxSize != 0));
  if (!rotate) {
    switch (rollPeriod) {
      case ROLL_DAILY:
        rotate = timeinfo.tm_mday != lastRollTime &&
                 static_cast<uint>(timeinfo.tm_hour) >= rollHour &&
                 static_cast<uint>(timeinfo.tm_min) >= rollMinute;
        break;
      case ROLL_HOURLY:
        rotate = timeinfo.tm_hour != lastRollTime &&
                 static_cast<uint>(timeinfo.tm_min) >= rollMinute;
        break;
      case ROLL_OTHER:
        rotate = rawtime >= lastRollTime + rollPeriodLength;
        break;
      case ROLL_NEVER:
        break;
    }
  }

  if (rotate) {
    rotateFile(rawtime);
  }
}

void FileStoreBase::rotateFile(time_t currentTime) {
  struct tm timeinfo;

  currentTime = currentTime > 0 ? currentTime : time(NULL);
  localtime_r(&currentTime, &timeinfo);

  LOG_OPER("[%s] %d:%d rotating file <%s> old size <%lu> max size <%lu>",
           categoryHandled.c_str(), timeinfo.tm_hour, timeinfo.tm_min,
           makeBaseFilename(&timeinfo).c_str(), currentSize, maxSize);

  printStats();
  openInternal(true, &timeinfo);
}

string FileStoreBase::makeFullFilename(int suffix, struct tm* creation_time,
                                       bool use_full_path) {

  ostringstream filename;

  if (use_full_path) {
    filename << filePath << '/';
  }
  filename << makeBaseFilename(creation_time);
  filename << '_' << setw(5) << setfill('0') << suffix;

  return filename.str();
}

string FileStoreBase::makeBaseSymlink() {
  ostringstream base;
  if (!baseSymlinkName.empty()) {
    base << baseSymlinkName << "_current";
  } else {
    base << baseFileName << "_current";
  }
  return base.str();
}

string FileStoreBase::makeFullSymlink() {
  ostringstream filename;
  filename << filePath << '/' << makeBaseSymlink();
  return filename.str();
}

string FileStoreBase::makeBaseFilename(struct tm* creation_time) {
  ostringstream filename;

  filename << baseFileName;
  if (rollPeriod != ROLL_NEVER) {
    filename << '-' << creation_time->tm_year + 1900  << '-'
             << setw(2) << setfill('0') << creation_time->tm_mon + 1 << '-'
             << setw(2) << setfill('0')  << creation_time->tm_mday;

  }
  return filename.str();
}

// returns the suffix of the newest file matching base_filename
int FileStoreBase::findNewestFile(const string& base_filename) {

  std::vector<std::string> files = FileInterface::list(filePath, fsType);

  int max_suffix = -1;
  std::string retval;
  for (std::vector<std::string>::iterator iter = files.begin();
       iter != files.end();
       ++iter) {

    int suffix = getFileSuffix(*iter, base_filename);
    if (suffix > max_suffix) {
      max_suffix = suffix;
    }
  }
  return max_suffix;
}

int FileStoreBase::findOldestFile(const string& base_filename) {

  std::vector<std::string> files = FileInterface::list(filePath, fsType);

  int min_suffix = -1;
  std::string retval;
  for (std::vector<std::string>::iterator iter = files.begin();
       iter != files.end();
       ++iter) {

    int suffix = getFileSuffix(*iter, base_filename);
    if (suffix >= 0 &&
        (min_suffix == -1 || suffix < min_suffix)) {
      min_suffix = suffix;
    }
  }
  return min_suffix;
}

int FileStoreBase::getFileSuffix(const string& filename, const string& base_filename) {
  int suffix = -1;
  string::size_type suffix_pos = filename.rfind('_');

  bool retVal = (0 == filename.substr(0, suffix_pos).compare(base_filename));

  if (string::npos != suffix_pos &&
      filename.length() > suffix_pos &&
      retVal) {
    stringstream stream;
    stream << filename.substr(suffix_pos + 1);
    stream >> suffix;
  }
  return suffix;
}

void FileStoreBase::printStats() {
  if (!writeStats) {
    return;
  }

  string filename(filePath);
  filename += "/scribe_stats";

  boost::shared_ptr<FileInterface> stats_file = FileInterface::createFileInterface(fsType, filename);
  if (!stats_file ||
      !stats_file->createDirectory(filePath) ||
      !stats_file->openWrite()) {
    LOG_OPER("[%s] Failed to open stats file <%s> of type <%s> for writing",
             categoryHandled.c_str(), filename.c_str(), fsType.c_str());
    // This isn't enough of a problem to change our status
    return;
  }

  time_t rawtime = time(NULL);
  struct tm timeinfo;
  localtime_r(&rawtime, &timeinfo);

  ostringstream msg;
  msg << timeinfo.tm_year + 1900  << '-'
      << setw(2) << setfill('0') << timeinfo.tm_mon + 1 << '-'
      << setw(2) << setfill('0') << timeinfo.tm_mday << '-'
      << setw(2) << setfill('0') << timeinfo.tm_hour << ':'
      << setw(2) << setfill('0') << timeinfo.tm_min;

  msg << " wrote <" << currentSize << "> bytes in <" << eventsWritten
      << "> events to file <" << currentFilename << ">" << endl;

  stats_file->write(msg.str());
  stats_file->close();
}

// Returns the number of bytes to pad to align to the specified chunk size
unsigned long FileStoreBase::bytesToPad(unsigned long next_message_length,
                                        unsigned long current_file_size,
                                        unsigned long chunk_size) {

  if (chunk_size > 0) {
    unsigned long space_left_in_chunk = chunk_size - current_file_size % chunk_size;
    if (next_message_length > space_left_in_chunk) {
      return space_left_in_chunk;
    } else {
      return 0;
    }
  }
  // chunk_size <= 0 means don't do any chunking
  return 0;
}

// set subDirectory to the name of this machine
void FileStoreBase::setHostNameSubDir() {
  if (!subDirectory.empty()) {
    string error_msg = "WARNING: Bad config - ";
    error_msg += "use_hostname_sub_directory will override sub_directory path";
    LOG_OPER("[%s] %s", categoryHandled.c_str(), error_msg.c_str());
  }

  char hostname[255];
  int error = gethostname(hostname, sizeof(hostname));
  if (error) {
    LOG_OPER("[%s] WARNING: gethostname returned error: %d ",
             categoryHandled.c_str(), error);
  }

  string hoststring(hostname);

  if (hoststring.empty()) {
    LOG_OPER("[%s] WARNING: could not get host name",
             categoryHandled.c_str());
  } else {
    subDirectory = hoststring;
  }
}

