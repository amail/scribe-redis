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

#ifndef SCRIBE_STORE_NETWORK_H
#define SCRIBE_STORE_NETWORK_H


#include "common.h" // includes std libs, thrift, and stl typedefs
#include "conf.h"
#include "file.h"
#include "conn_pool.h"

/*
 * This store sends messages to another scribe server.
 * This class is really just an adapter to the global
 * connection pool g_connPool.
 */
class NetworkStore : public Store {

 public:
  NetworkStore(const std::string& category, bool multi_category,
               const std::string& trigger_path);
  ~NetworkStore();

  boost::shared_ptr<Store> copy(const std::string &category);
  bool handleMessages(boost::shared_ptr<logentry_vector_t> messages);
  bool open();
  bool isOpen();
  void configure(pStoreConf configuration);
  void close();
  void flush();

 protected:
  static const long int DEFAULT_SOCKET_TIMEOUT_MS = 5000; // 5 sec timeout

  // configuration
  bool useConnPool;
  bool smcBased;
  long int timeout;
  std::string remoteHost;
  unsigned long remotePort; // long because it works with config code
  std::string smcService;
  std::string serviceOptions;
  server_vector_t servers;
  unsigned long serviceCacheTimeout;
  time_t lastServiceCheck;

  // state
  bool opened;
  boost::shared_ptr<scribeConn> unpooledConn; // null if useConnPool

 private:
  // disallow copy, assignment, and empty construction
  NetworkStore();
  NetworkStore(NetworkStore& rhs);
  NetworkStore& operator=(NetworkStore& rhs);
};

#endif // SCRIBE_STORE_NETWORK_H

