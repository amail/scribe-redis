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
#include "scribe_server.h"

#ifndef USE_REDIS_ONLY
#include "store_filebase.h"
#include "store_file.h"
#include "store_buffer.h"
#include "store_network.h"
#include "store_bucket.h"
#include "store_thriftfile.h"
#include "store_null.h"
#include "store_multi.h"
#include "store_category.h"
#include "store_multifile.h"
#include "store_thriftmultifile.h"
#endif

#include "store_redis.h"


using namespace std;
using namespace boost;
using namespace boost::filesystem;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;
using namespace scribe::thrift;

boost::shared_ptr<Store>
Store::createStore(const string& type, const string& category,
                   bool readable, bool multi_category, const string& trigger_path) {
                   
  if (0 == type.compare("redis")) {
    return shared_ptr<Store>(new RedisStore(category, multi_category, trigger_path));
  #ifndef USE_REDIS_ONLY
  } else if (0 == type.compare("file")) {
    return shared_ptr<Store>(new FileStore(category, multi_category, trigger_path, readable));
  } else if (0 == type.compare("buffer")) {
    return shared_ptr<Store>(new BufferStore(category, multi_category, trigger_path));
  } else if (0 == type.compare("network")) {
    return shared_ptr<Store>(new NetworkStore(category, multi_category, trigger_path));
  } else if (0 == type.compare("bucket")) {
    return shared_ptr<Store>(new BucketStore(category, multi_category, trigger_path));
  } else if (0 == type.compare("thriftfile")) {
    return shared_ptr<Store>(new ThriftFileStore(category, multi_category, trigger_path));
  } else if (0 == type.compare("null")) {
    return shared_ptr<Store>(new NullStore(category, multi_category, trigger_path));
  } else if (0 == type.compare("multi")) {
    return shared_ptr<Store>(new MultiStore(category, multi_category, trigger_path));
  } else if (0 == type.compare("category")) {
    return shared_ptr<Store>(new CategoryStore(category, multi_category, trigger_path));
  } else if (0 == type.compare("multifile")) {
    return shared_ptr<Store>(new MultiFileStore(category, multi_category, trigger_path));
  } else if (0 == type.compare("thriftmultifile")) {
    return shared_ptr<Store>(new ThriftMultiFileStore(category, multi_category, trigger_path));
  #endif
  } else {
    return shared_ptr<Store>();
  }
}

Store::Store(const string& category, const string &type, bool multi_category, const string& trigger_path)
  : categoryHandled(category),
    multiCategory(multi_category),
    storeType(type),
    triggerPath(trigger_path) {
  pthread_mutex_init(&statusMutex, NULL);
}

Store::~Store() {
  pthread_mutex_destroy(&statusMutex);
}

void Store::setStatus(const std::string& new_status) {
  pthread_mutex_lock(&statusMutex);
  status = new_status;
  pthread_mutex_unlock(&statusMutex);
}

std::string Store::getStatus() {
  pthread_mutex_lock(&statusMutex);
  std::string return_status(status);
  pthread_mutex_unlock(&statusMutex);
  return return_status;
}

bool Store::readOldest(/*out*/ boost::shared_ptr<logentry_vector_t> messages,
                       struct tm* now) {
  LOG_OPER("[%s] ERROR: attempting to read from a write-only store", categoryHandled.c_str());
  return false;
}

bool Store::replaceOldest(boost::shared_ptr<logentry_vector_t> messages,
                          struct tm* now) {
  LOG_OPER("[%s] ERROR: attempting to read from a write-only store", categoryHandled.c_str());
  return false;
}

void Store::deleteOldest(struct tm* now) {
   LOG_OPER("[%s] ERROR: attempting to read from a write-only store", categoryHandled.c_str());
}

bool Store::empty(struct tm* now) {
  LOG_OPER("[%s] ERROR: attempting to read from a write-only store", categoryHandled.c_str());
  return true;
}

const std::string& Store::getType() {
  return storeType;
}

bool Store::runTrigger(const std::string& message) {
  if(!triggerPath.empty()) {
    pid_t pID = fork();
    if (pID == 0)
    {
      execl(triggerPath.c_str(), triggerPath.c_str(), categoryHandled.c_str(), message.c_str(), (char *)0);
      exit(0);
    }
    return true;
  }
  return false;
}

