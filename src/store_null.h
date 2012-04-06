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

#ifndef SCRIBE_STORE_NULL_H
#define SCRIBE_STORE_NULL_H


#include "common.h" // includes std libs, thrift, and stl typedefs
#include "conf.h"
#include "file.h"
#include "conn_pool.h"

/*
 * This store intentionally left blank.
 */
class NullStore : public Store {

 public:
  NullStore(const std::string& category, bool multi_category,
            const std::string& trigger_path);
  virtual ~NullStore();

  boost::shared_ptr<Store> copy(const std::string &category);
  bool open();
  bool isOpen();
  void configure(pStoreConf configuration);
  void close();

  bool handleMessages(boost::shared_ptr<logentry_vector_t> messages);
  void flush();

  // null stores are readable, but you never get anything
  virtual bool readOldest(/*out*/ boost::shared_ptr<logentry_vector_t> messages,                          struct tm* now);
  virtual bool replaceOldest(boost::shared_ptr<logentry_vector_t> messages,
                             struct tm* now);
  virtual void deleteOldest(struct tm* now);
  virtual bool empty(struct tm* now);


 private:
  // disallow empty constructor, copy and assignment
  NullStore();
  NullStore(Store& rhs);
  NullStore& operator=(Store& rhs);
};

#endif // SCRIBE_STORE_NULL_H

