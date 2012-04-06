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
// @author Joel Seligstein

#ifndef SCRIBE_STORE_MULTI_H
#define SCRIBE_STORE_MULTI_H


#include "common.h" // includes std libs, thrift, and stl typedefs
#include "conf.h"
#include "file.h"
#include "conn_pool.h"

/*
 * This store relays messages to n other stores
 * @author Joel Seligstein
 */
class MultiStore : public Store {
 public:
  MultiStore(const std::string& category, bool multi_category,
             const std::string& trigger_path);
  ~MultiStore();

  boost::shared_ptr<Store> copy(const std::string &category);
  bool open();
  bool isOpen();
  void configure(pStoreConf configuration);
  void close();

  bool handleMessages(boost::shared_ptr<logentry_vector_t> messages);
  void periodicCheck();
  void flush();

  // read won't make sense since we don't know which store to read from
  bool readOldest(/*out*/ boost::shared_ptr<logentry_vector_t> messages,
                  struct tm* now) { return false; }
  void deleteOldest(struct tm* now) {}
  bool empty(struct tm* now) { return true; }

 protected:
  std::vector<boost::shared_ptr<Store> > stores;
  enum report_success_value {
    SUCCESS_ANY = 1,
    SUCCESS_ALL
  };
  report_success_value report_success;

 private:
  // disallow copy, assignment, and empty construction
  MultiStore();
  MultiStore(Store& rhs);
  MultiStore& operator=(Store& rhs);
};

#endif // SCRIBE_STORE_MULTI_H

