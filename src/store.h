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

#ifndef SCRIBE_STORE_H
#define SCRIBE_STORE_H

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
 * Abstract class to define the interface for a store
 * and implement some basic functionality.
 */
class Store {
 public:
  // Creates an object of the appropriate subclass.
  static boost::shared_ptr<Store>
    createStore(const std::string& type, const std::string& category,
                bool readable = false, bool multi_category = false,
                const std::string& trigger_path = "");

  Store(const std::string& category, const std::string &type,
        bool multi_category = false, const std::string& trigger_path = "");
  virtual ~Store();

  virtual boost::shared_ptr<Store> copy(const std::string &category) = 0;
  virtual bool open() = 0;
  virtual bool isOpen() = 0;
  virtual void configure(pStoreConf configuration) = 0;
  virtual void close() = 0;

  // Attempts to store messages and returns true if successful.
  // On failure, returns false and messages contains any un-processed messages
  virtual bool handleMessages(boost::shared_ptr<logentry_vector_t> messages) = 0;
  virtual void periodicCheck() {}
  virtual void flush() = 0;

  virtual std::string getStatus();

  // following methods must be overidden to make a store readable
  virtual bool readOldest(/*out*/ boost::shared_ptr<logentry_vector_t> messages,
                          struct tm* now);
  virtual void deleteOldest(struct tm* now);
  virtual bool replaceOldest(boost::shared_ptr<logentry_vector_t> messages,
                             struct tm* now);
  virtual bool empty(struct tm* now);

  // don't need to override
  virtual const std::string& getType();

 protected:
  virtual void setStatus(const std::string& new_status);
  virtual bool runTrigger(const std::string& message);
  std::string categoryHandled;
  bool multiCategory;             // Whether multiple categories are handled
  std::string storeType;
  bool useTriggers;
  std::string status;
  std::string triggerPath;

  // Don't ever take this lock for multiple stores at the same time
  pthread_mutex_t statusMutex;

 private:
  // disallow copy, assignment, and empty construction
  Store(Store& rhs);
  Store& operator=(Store& rhs);
};

#endif // SCRIBE_STORE_H

