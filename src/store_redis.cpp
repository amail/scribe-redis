//  Copyright (c) 2012 Comfirm AB
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
//
// @author Jack Engqvist Johansson

#include "store.h"
#include "store_redis.h"

using namespace std;
using namespace boost;

RedisStore::RedisStore(const std::string& category, bool multi_category,
                       const string& trigger_path)
  : Store(category, "null", multi_category, trigger_path),
  redisHost("localhost"),
  redisPort(6379)
{}

RedisStore::~RedisStore() {
}

boost::shared_ptr<Store> RedisStore::copy(const std::string &category) {
  RedisStore *store = new RedisStore(category, multiCategory, triggerPath);
  shared_ptr<Store> copied = shared_ptr<Store>(store);
  return copied;
}

bool RedisStore::open() {
  struct timeval timeout = { 1, 500000 }; // 1.5 seconds
  try {
      c = redisConnectWithTimeout(redisHost.c_str(), redisPort, timeout);
      if (c->err) {
        printf("Could not connect to redis: %s\n", c->errstr);
        return false;
      }
  } catch(std::exception const& e) {
      LOG_OPER("Could not connect to redis: %s", e.what());
      return false;
  }
  
  return true;
}

bool RedisStore::isOpen() {
  return false;
}

void RedisStore::configure(pStoreConf configuration) {
  // Redis connection settings
  configuration->getString("redis_host", redisHost);
  configuration->getUnsigned("redis_port", redisPort);
}

void RedisStore::close() {
	redisFree(c);	
}

bool RedisStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
  unsigned long int leng_key;
  unsigned long int leng_msg;
  unsigned long int leng_cmd;
  redisReply *reply;

  // Key length
  leng_key = categoryHandled.length() + 19;
  leng_cmd = leng_key + 10;
  char key[leng_key];
  
  // Generate key
  time_t rawtime;
  struct tm* local;
  time ( &rawtime );
  local = localtime(&rawtime);
  int year = local->tm_year;
  int month = local->tm_mon;
  int day = local->tm_mday;
  int hour = local->tm_hour;
  
  sprintf(key, "log:%d:%d:%d:%d:%s", year + 1900, month + 1, day, hour, categoryHandled.c_str());
  
  // Open redis connection
  open();
  
  // Log messages
  for (logentry_vector_t::iterator iter = messages->begin();
       iter != messages->end();
       ++iter) {
       
    std::string message = (*iter)->message;
    leng_msg = message.length();
    char msg[leng_msg];
    memcpy(&msg, message.c_str(), leng_msg);
    msg[leng_msg - 1] = '\0';
    
    char cmd[leng_cmd];
    sprintf(cmd, "LPUSH %s %%s", key);
    
    try {
      reply = (redisReply*)redisCommand(c, cmd, msg);
    } catch(std::exception const& e) {
      LOG_OPER("Could not connect to redis: %s", e.what());
      return false;
    }

    freeReplyObject(reply);
    runTrigger(message);
  }
  
  // Close redis connection
  close();

  return true;
}

void RedisStore::flush() {
}

bool RedisStore::readOldest(/*out*/ boost::shared_ptr<logentry_vector_t> messages,
                       struct tm* now) {
  return true;
}

bool RedisStore::replaceOldest(boost::shared_ptr<logentry_vector_t> messages,
                              struct tm* now) {
  return true;
}

void RedisStore::deleteOldest(struct tm* now) {
}

bool RedisStore::empty(struct tm* now) {
  return true;
}

