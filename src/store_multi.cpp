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
#include "store.h"
#include "store_multi.h"

using namespace std;
using namespace boost;

MultiStore::MultiStore(const std::string& category, bool multi_category,
                       const string& trigger_path)
  : Store(category, "multi", multi_category, trigger_path) {
}

MultiStore::~MultiStore() {
}

boost::shared_ptr<Store> MultiStore::copy(const std::string &category) {
  MultiStore *store = new MultiStore(category, multiCategory, triggerPath);
  store->report_success = this->report_success;
  boost::shared_ptr<Store> tmp_copy;
  for (std::vector<boost::shared_ptr<Store> >::iterator iter = stores.begin();
       iter != stores.end();
       ++iter) {
    tmp_copy = (*iter)->copy(category);
    store->stores.push_back(tmp_copy);
  }

  return shared_ptr<Store>(store);
}

bool MultiStore::open() {
  bool all_result = true;
  bool any_result = false;
  bool cur_result;
  for (std::vector<boost::shared_ptr<Store> >::iterator iter = stores.begin();
       iter != stores.end();
       ++iter) {
    cur_result = (*iter)->open();
    any_result |= cur_result;
    all_result &= cur_result;
  }
  return (report_success == SUCCESS_ALL) ? all_result : any_result;
}

bool MultiStore::isOpen() {
  bool all_result = true;
  bool any_result = false;
  bool cur_result;
  for (std::vector<boost::shared_ptr<Store> >::iterator iter = stores.begin();
       iter != stores.end();
       ++iter) {
    cur_result = (*iter)->isOpen();
    any_result |= cur_result;
    all_result &= cur_result;
  }
  return (report_success == SUCCESS_ALL) ? all_result : any_result;
}

void MultiStore::configure(pStoreConf configuration) {
  /**
   * in this store, we look for other numbered stores
   * in the following fashion:
   * <store>
   *   type=multi
   *   report_success=all|any
   *   <store0>
   *     ...
   *   </store0>
       ...
   *   <storen>
   *     ...
   *   </storen>
   * </store>
   */
  pStoreConf cur_conf;
  string cur_type;
  boost::shared_ptr<Store> cur_store;
  string report_preference;

  // find reporting preference
  if (configuration->getString("report_success", report_preference)) {
    if (0 == report_preference.compare("all")) {
      report_success = SUCCESS_ALL;
      LOG_OPER("[%s] MULTI: Logging success only if all stores succeed.",
               categoryHandled.c_str());
    } else if (0 == report_preference.compare("any")) {
      report_success = SUCCESS_ANY;
      LOG_OPER("[%s] MULTI: Logging success if any store succeeds.",
               categoryHandled.c_str());
    } else {
      LOG_OPER("[%s] MULTI: %s is an invalid value for report_success.",
               categoryHandled.c_str(), report_preference.c_str());
      setStatus("MULTI: Invalid report_success value.");
      return;
    }
  } else {
    report_success = SUCCESS_ALL;
  }

  // find stores
  for (int i=0; ;++i) {
    stringstream ss;
    ss << "store" << i;
    if (!configuration->getStore(ss.str(), cur_conf)) {
      // allow this to be 0 or 1 indexed
      if (i == 0) {
        continue;
      }

      // no store for this id? we're finished.
      break;
    } else {
      // find this store's type
      if (!cur_conf->getString("type", cur_type)) {
        LOG_OPER("[%s] MULTI: Store %d is missing type.", categoryHandled.c_str(), i);
        setStatus("MULTI: Store is missing type.");
        return;
      } else {
        // add it to the list
        cur_store = createStore(cur_type, categoryHandled, false, multiCategory);
        LOG_OPER("[%s] MULTI: Configured store of type %s successfully.",
                 categoryHandled.c_str(), cur_type.c_str());
        cur_store->configure(cur_conf);
        stores.push_back(cur_store);
      }
    }
  }

  if (stores.size() == 0) {
    setStatus("MULTI: No stores found, invalid store.");
    LOG_OPER("[%s] MULTI: No stores found, invalid store.", categoryHandled.c_str());
  }
}

void MultiStore::close() {
  for (std::vector<boost::shared_ptr<Store> >::iterator iter = stores.begin();
       iter != stores.end();
       ++iter) {
    (*iter)->close();
  }
}

bool MultiStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
  bool all_result = true;
  bool any_result = false;
  bool cur_result;
  for (std::vector<boost::shared_ptr<Store> >::iterator iter = stores.begin();
       iter != stores.end();
       ++iter) {
    cur_result = (*iter)->handleMessages(messages);
    any_result |= cur_result;
    all_result &= cur_result;
  }

  // We cannot accurately report the number of messages not handled as messages
  // can be partially handled by a subset of stores.  So a multistore failure
  // will over-record the number of lost messages.
  return (report_success == SUCCESS_ALL) ? all_result : any_result;
}

void MultiStore::periodicCheck() {
  for (std::vector<boost::shared_ptr<Store> >::iterator iter = stores.begin();
       iter != stores.end();
       ++iter) {
    (*iter)->periodicCheck();
  }
}

void MultiStore::flush() {
  for (std::vector<boost::shared_ptr<Store> >::iterator iter = stores.begin();
       iter != stores.end();
       ++iter) {
    (*iter)->flush();
  }
}

