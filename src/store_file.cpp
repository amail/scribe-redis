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
#include "store_file.h"

using namespace std;
using namespace boost;
using namespace scribe::thrift;

const string meta_logfile_prefix = "scribe_meta<new_logfile>: ";

FileStore::FileStore(const string& category, bool multi_category,
                     const string& trigger_path, bool is_buffer_file)
  : FileStoreBase(category, "file", multi_category, trigger_path),
    isBufferFile(is_buffer_file),
    addNewlines(false) {
}

FileStore::~FileStore() {
}

void FileStore::configure(pStoreConf configuration) {
  FileStoreBase::configure(configuration);

  // We can run using defaults for all of these, but there are
  // a couple of suspicious things we warn about.
  if (isBufferFile) {
    // scheduled file rotations of buffer files lead to too many messy cases
    rollPeriod = ROLL_NEVER;

    // Chunks don't work with the buffer file. There's no good reason
    // for this, it's just that the FileStore handles chunk padding and
    // the FileInterface handles framing, and you need to look at both to
    // read a file that's both chunked and framed. The buffer file has
    // to be framed, so we don't allow it to be chunked.
    // (framed means we write a message size to disk before the message
    //  data, which allows us to identify separate messages in binary data.
    //  Chunked means we pad with zeroes to ensure that every multiple
    //  of n bytes is the start of a message, which helps in recovering
    //  corrupted binary data and seeking into large files)
    chunkSize = 0;

    // Combine all categories in a single file for buffers
    if (multiCategory) {
      writeCategory = true;
    }
  }

  unsigned long inttemp = 0;
  configuration->getUnsigned("add_newlines", inttemp);
  addNewlines = inttemp ? true : false;
}

bool FileStore::openInternal(bool incrementFilename, struct tm* current_time) {
  bool success = false;
  struct tm timeinfo;

  if (!current_time) {
    time_t rawtime = time(NULL);
    localtime_r(&rawtime, &timeinfo);
    current_time = &timeinfo;
  }

  try {
    int suffix = findNewestFile(makeBaseFilename(current_time));

    if (incrementFilename) {
      ++suffix;
    }

    // this is the case where there's no file there and we're not incrementing
    if (suffix < 0) {
      suffix = 0;
    }

    string file = makeFullFilename(suffix, current_time);

    switch (rollPeriod) {
      case ROLL_DAILY:
        lastRollTime = current_time->tm_mday;
        break;
      case ROLL_HOURLY:
        lastRollTime = current_time->tm_hour;
        break;
      case ROLL_OTHER:
        lastRollTime = time(NULL);
        break;
      case ROLL_NEVER:
        break;
    }

    if (writeFile) {
      if (writeMeta) {
        writeFile->write(meta_logfile_prefix + file);
      }
      writeFile->close();
    }

    writeFile = FileInterface::createFileInterface(fsType, file, isBufferFile);
    if (!writeFile) {
      LOG_OPER("[%s] Failed to create file <%s> of type <%s> for writing",
               categoryHandled.c_str(), file.c_str(), fsType.c_str());
      setStatus("file open error");
      return false;
    }

    success = writeFile->createDirectory(baseFilePath);

    // If we created a subdirectory, we need to create two directories
    if (success && !subDirectory.empty()) {
      success = writeFile->createDirectory(filePath);
    }

    if (!success) {
      LOG_OPER("[%s] Failed to create directory for file <%s>",
               categoryHandled.c_str(), file.c_str());
      setStatus("File open error");
      return false;
    }

    success = writeFile->openWrite();


    if (!success) {
      LOG_OPER("[%s] Failed to open file <%s> for writing",
              categoryHandled.c_str(),
              file.c_str());
      setStatus("File open error");
    } else {

      /* just make a best effort here, and don't error if it fails */
      if (createSymlink && !isBufferFile) {
        string symlinkName = makeFullSymlink();
        boost::shared_ptr<FileInterface> tmp =
          FileInterface::createFileInterface(fsType, symlinkName, isBufferFile);
        tmp->deleteFile();
        string symtarget = makeFullFilename(suffix, current_time, false);
        writeFile->createSymlink(symtarget, symlinkName);
      }
      // else it confuses the filename code on reads

      LOG_OPER("[%s] Opened file <%s> for writing", categoryHandled.c_str(),
              file.c_str());

      currentSize = writeFile->fileSize();
      currentFilename = file;
      eventsWritten = 0;
      setStatus("");
    }

  } catch(std::exception const& e) {
    LOG_OPER("[%s] Failed to create/open file of type <%s> for writing",
             categoryHandled.c_str(), fsType.c_str());
    LOG_OPER("Exception: %s", e.what());
    setStatus("file create/open error");

    return false;
  }
  return success;
}

bool FileStore::isOpen() {
  return writeFile && writeFile->isOpen();
}

void FileStore::close() {
  if (writeFile) {
    writeFile->close();
  }
}

void FileStore::flush() {
  if (writeFile) {
    writeFile->flush();
  }
}

shared_ptr<Store> FileStore::copy(const std::string &category) {
  FileStore *store = new FileStore(category, multiCategory, triggerPath, isBufferFile);
  shared_ptr<Store> copied = shared_ptr<Store>(store);

  store->addNewlines = addNewlines;
  store->copyCommon(this);
  return copied;
}

bool FileStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {

  if (!isOpen()) {
    LOG_OPER("[%s] File failed to open FileStore::handleMessages()", categoryHandled.c_str());
    return false;
  }

  // write messages to current file
  return writeMessages(messages);
}

// writes messages to either the specified file or the the current writeFile
bool FileStore::writeMessages(boost::shared_ptr<logentry_vector_t> messages,
                              boost::shared_ptr<FileInterface> file) {
  // Data is written to a buffer first, then sent to disk in one call to write.
  // This costs an extra copy of the data, but dramatically improves latency with
  // network based files. (nfs, etc)
  string        write_buffer;
  bool          success = true;
  unsigned long current_size_buffered = 0; // size of data in write_buffer
  unsigned long num_buffered = 0;
  unsigned long num_written = 0;
  boost::shared_ptr<FileInterface> write_file;
  unsigned long max_write_size = min(maxSize, maxWriteSize);

  // if no file given, use current writeFile
  if (file) {
    write_file = file;
  } else {
    write_file = writeFile;
  }

  try {
    for (logentry_vector_t::iterator iter = messages->begin();
         iter != messages->end();
         ++iter) {

      // have to be careful with the length here. getFrame wants the length without
      // the frame, then bytesToPad wants the length of the frame and the message.
      unsigned long length = 0;
      unsigned long message_length = (*iter)->message.length();
      string frame, category_frame;

      if (addNewlines) {
        ++message_length;
      }

      length += message_length;

      if (writeCategory) {
        //add space for category+newline and category frame
        unsigned long category_length = (*iter)->category.length() + 1;
        length += category_length;

        category_frame = write_file->getFrame(category_length);
        length += category_frame.length();
      }

      // frame is a header that the underlying file class can add to each message
      frame = write_file->getFrame(message_length);

      length += frame.length();

      // padding to align messages on chunk boundaries
      unsigned long padding = bytesToPad(length, current_size_buffered, chunkSize);

      length += padding;

      if (padding) {
        write_buffer += string(padding, 0);
      }

      if (writeCategory) {
        write_buffer += category_frame;
        write_buffer += (*iter)->category + "\n";
      }

      write_buffer += frame;
      write_buffer += (*iter)->message;

      if (addNewlines) {
        write_buffer += "\n";
      }

      current_size_buffered += length;
      num_buffered++;

      // Write buffer if processing last message or if larger than allowed
      if ((currentSize + current_size_buffered > max_write_size && maxSize != 0) ||
          messages->end() == iter + 1 ) {
        if (!write_file->write(write_buffer)) {
          LOG_OPER("[%s] File store failed to write (%lu) messages to file",
                   categoryHandled.c_str(), messages->size());
          setStatus("File write error");
          success = false;
          break;
        }

        num_written += num_buffered;
        currentSize += current_size_buffered;
        num_buffered = 0;
        current_size_buffered = 0;
        write_buffer = "";
      }

      // rotate file if large enough and not writing to a separate file
      if ((currentSize > maxSize && maxSize != 0 )&& !file) {
        rotateFile();
        write_file = writeFile;
      }
    }
  } catch (std::exception const& e) {
    LOG_OPER("[%s] File store failed to write. Exception: %s",
             categoryHandled.c_str(), e.what());
    success = false;
  }

  eventsWritten += num_written;

  if (!success) {
    close();

    // update messages to include only the messages that were not handled
    if (num_written > 0) {
      messages->erase(messages->begin(), messages->begin() + num_written);
    }
  }

  return success;
}

void FileStore::deleteOldest(struct tm* now) {

  int index = findOldestFile(makeBaseFilename(now));
  if (index < 0) {
    return;
  }
  shared_ptr<FileInterface> deletefile = FileInterface::createFileInterface(fsType,
                                            makeFullFilename(index, now));
  deletefile->deleteFile();
}

// Replace the messages in the oldest file at this timestamp with the input messages
bool FileStore::replaceOldest(boost::shared_ptr<logentry_vector_t> messages,
                              struct tm* now) {
  string base_name = makeBaseFilename(now);
  int index = findOldestFile(base_name);
  if (index < 0) {
    LOG_OPER("[%s] Could not find files <%s>", categoryHandled.c_str(), base_name.c_str());
    return false;
  }

  string filename = makeFullFilename(index, now);

  // Need to close and reopen store in case we already have this file open
  close();

  shared_ptr<FileInterface> infile = FileInterface::createFileInterface(fsType,
                                          filename, isBufferFile);

  // overwrite the old contents of the file
  bool success;
  if (infile->openTruncate()) {
    success = writeMessages(messages, infile);

  } else {
    LOG_OPER("[%s] Failed to open file <%s> for writing and truncate",
             categoryHandled.c_str(), filename.c_str());
    success = false;
  }

  // close this file and re-open store
  infile->close();
  open();

  return success;
}

bool FileStore::readOldest(/*out*/ boost::shared_ptr<logentry_vector_t> messages,
                           struct tm* now) {

  int index = findOldestFile(makeBaseFilename(now));
  if (index < 0) {
    // This isn't an error. It's legit to call readOldest when there aren't any
    // files left, in which case the call succeeds but returns messages empty.
    return true;
  }
  std::string filename = makeFullFilename(index, now);

  shared_ptr<FileInterface> infile = FileInterface::createFileInterface(fsType, filename, isBufferFile);

  if (!infile->openRead()) {
    LOG_OPER("[%s] Failed to open file <%s> for reading", categoryHandled.c_str(), filename.c_str());
    return false;
  }

  std::string message;
  while (infile->readNext(message)) {
    if (!message.empty()) {
      logentry_ptr_t entry = logentry_ptr_t(new LogEntry);

      // check whether a category is stored with the message
      if (writeCategory) {
        // get category without trailing \n
        entry->category = message.substr(0, message.length() - 1);

        if (!infile->readNext(message)) {
          LOG_OPER("[%s] category not stored with message <%s>",
                   categoryHandled.c_str(), entry->category.c_str());
        }
      } else {
        entry->category = categoryHandled;
      }

      entry->message = message;

      messages->push_back(entry);
    }
  }
  infile->close();

  LOG_OPER("[%s] successfully read <%lu> entries from file <%s>",
        categoryHandled.c_str(), messages->size(), filename.c_str());
  return true;
}

bool FileStore::empty(struct tm* now) {

  std::vector<std::string> files = FileInterface::list(filePath, fsType);

  std::string base_filename = makeBaseFilename(now);
  for (std::vector<std::string>::iterator iter = files.begin();
       iter != files.end();
       ++iter) {
    int suffix =  getFileSuffix(*iter, base_filename);
    if (-1 != suffix) {
      std::string fullname = makeFullFilename(suffix, now);
      shared_ptr<FileInterface> file = FileInterface::createFileInterface(fsType, fullname);
      if (file->fileSize()) {
        return false;
      }
    } // else it doesn't match the filename for this store
  }
  return true;
}

