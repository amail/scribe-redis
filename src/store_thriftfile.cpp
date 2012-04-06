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

#include "thrift/transport/TSimpleFileTransport.h"
#include "common.h"
#include "scribe_server.h"
#include "store.h"
#include "store_filebase.h"
#include "store_thriftfile.h"

using namespace std;
using namespace boost;
using namespace apache::thrift;
using namespace apache::thrift::transport;

ThriftFileStore::ThriftFileStore(const std::string& category, bool multi_category,
                                 const string& trigger_path)
  : FileStoreBase(category, "thriftfile", multi_category, trigger_path),
    flushFrequencyMs(0),
    msgBufferSize(0),
    useSimpleFile(0) {
}

ThriftFileStore::~ThriftFileStore() {
}

shared_ptr<Store> ThriftFileStore::copy(const std::string &category) {
  ThriftFileStore *store = new ThriftFileStore(category, multiCategory, triggerPath);
  shared_ptr<Store> copied = shared_ptr<Store>(store);

  store->flushFrequencyMs = flushFrequencyMs;
  store->msgBufferSize = msgBufferSize;
  store->copyCommon(this);
  return copied;
}

bool ThriftFileStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
  if (!isOpen()) {
    return false;
  }

  unsigned long messages_handled = 0;
  for (logentry_vector_t::iterator iter = messages->begin();
       iter != messages->end();
       ++iter) {

    // This length is an estimate -- what the ThriftLogFile actually writes is a black box to us
    uint32_t length = (*iter)->message.size();

    try {
      thriftFileTransport->write(reinterpret_cast<const uint8_t*>((*iter)->message.data()), length);
      currentSize += length;
      ++eventsWritten;
      ++messages_handled;
    } catch (TException te) {
      LOG_OPER("[%s] Thrift file store failed to write to file: %s\n", categoryHandled.c_str(), te.what());
      setStatus("File write error");

      // If we already handled some messages, remove them from vector before
      // returning failure
      if (messages_handled) {
        messages->erase(messages->begin(), iter);
      }
      return false;
    }
  }
  // We can't wait until periodicCheck because we could be getting
  // a lot of data all at once in a failover situation
  if (currentSize > maxSize && maxSize != 0) {
    rotateFile();
  }

  return true;
}

bool ThriftFileStore::open() {
  return openInternal(true, NULL);
}

bool ThriftFileStore::isOpen() {
  return thriftFileTransport && thriftFileTransport->isOpen();
}

void ThriftFileStore::configure(pStoreConf configuration) {
  FileStoreBase::configure(configuration);
  configuration->getUnsigned("flush_frequency_ms", flushFrequencyMs);
  configuration->getUnsigned("msg_buffer_size", msgBufferSize);
  configuration->getUnsigned("use_simple_file", useSimpleFile);
}

void ThriftFileStore::close() {
  thriftFileTransport.reset();
}

void ThriftFileStore::flush() {
  // TFileTransport has its own periodic flushing mechanism, and we
  // introduce deadlocks if we try to call it from more than one place
  return;
}

bool ThriftFileStore::openInternal(bool incrementFilename, struct tm* current_time) {
  struct tm timeinfo;

  if (!current_time) {
    time_t rawtime = time(NULL);
    localtime_r(&rawtime, &timeinfo);
    current_time = &timeinfo;
  }

  int suffix = findNewestFile(makeBaseFilename(current_time));

  if (incrementFilename) {
    ++suffix;
  }

  // this is the case where there's no file there and we're not incrementing
  if (suffix < 0) {
    suffix = 0;
  }

  string filename = makeFullFilename(suffix, current_time);
  /* try to create the directory containing the file */
  if (!createFileDirectory()) {
    LOG_OPER("[%s] Could not create path for file: %s",
             categoryHandled.c_str(), filename.c_str());
    return false;
  }

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


  try {
    if (useSimpleFile) {
      thriftFileTransport.reset(new TSimpleFileTransport(filename, false, true));
    } else {
      TFileTransport *transport = new TFileTransport(filename);
      thriftFileTransport.reset(transport);

      if (chunkSize != 0) {
	transport->setChunkSize(chunkSize);
      }
      if (flushFrequencyMs > 0) {
	transport->setFlushMaxUs(flushFrequencyMs * 1000);
      }
      if (msgBufferSize > 0) {
	transport->setEventBufferSize(msgBufferSize);
      }
    }

    LOG_OPER("[%s] Opened file <%s> for writing", categoryHandled.c_str(), filename.c_str());

    struct stat st;
    if (stat(filename.c_str(), &st) == 0) {
      currentSize = st.st_size;
    } else {
      currentSize = 0;
    }
    currentFilename = filename;
    eventsWritten = 0;
    setStatus("");
  } catch (TException te) {
    LOG_OPER("[%s] Failed to open file <%s> for writing: %s\n", categoryHandled.c_str(), filename.c_str(), te.what());
    setStatus("File open error");
    return false;
  }

  /* just make a best effort here, and don't error if it fails */
  if (createSymlink) {
    string symlinkName = makeFullSymlink();
    unlink(symlinkName.c_str());
    string symtarget = makeFullFilename(suffix, current_time, false);
    symlink(symtarget.c_str(), symlinkName.c_str());
  }

  return true;
}

bool ThriftFileStore::createFileDirectory () {
  try {
    boost::filesystem::create_directories(filePath);
  }catch(std::exception const& e) {
    LOG_OPER("Exception < %s > in ThriftFileStore::createFileDirectory for path %s",
      e.what(),filePath.c_str());
    return false;
  }
  return true;
}

