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
#include "store_category.h"

using namespace std;
using namespace boost;

CategoryStore::CategoryStore(const std::string& category, bool multiCategory,
                             const string& trigger_path)
  : Store(category, "category", multiCategory, trigger_path) {
}

CategoryStore::CategoryStore(const std::string& category,
                             const std::string& name, bool multiCategory,
                             const string& trigger_path)
  : Store(category, name, multiCategory, trigger_path) {
}

CategoryStore::~CategoryStore() {
}

boost::shared_ptr<Store> CategoryStore::copy(const std::string &category) {
  CategoryStore *store = new CategoryStore(category, multiCategory, triggerPath);

  store->modelStore = modelStore->copy(category);

  return shared_ptr<Store>(store);
}

bool CategoryStore::open() {
  bool result = true;

  for (map<string, shared_ptr<Store> >::iterator iter = stores.begin();
      iter != stores.end();
      ++iter) {
    result &= iter->second->open();
  }

  return result;
}

bool CategoryStore::isOpen() {

  for (map<string, shared_ptr<Store> >::iterator iter = stores.begin();
      iter != stores.end();
      ++iter) {
    if (!iter->second->isOpen()) {
      return false;
    }
  }

  return true;
}

void CategoryStore::configure(pStoreConf configuration) {
  /**
   *  Parse the store defined and use this store as a model to create a
   *  new store for every new category we see later.
   *  <store>
   *    type=category
   *    <model>
   *      type=...
   *      ...
   *    </model>
   *  </store>
   */
  pStoreConf cur_conf;

  if (!configuration->getStore("model", cur_conf)) {
    setStatus("CATEGORYSTORE: NO stores found, invalid store.");
    LOG_OPER("[%s] CATEGORYSTORE: No stores found, invalid store.",
             categoryHandled.c_str());
  } else {
    string cur_type;

    // find this store's type
    if (!cur_conf->getString("type", cur_type)) {
      LOG_OPER("[%s] CATEGORYSTORE: Store is missing type.",
               categoryHandled.c_str());
      setStatus("CATEGORYSTORE: Store is missing type.");
      return;
    }

    configureCommon(cur_conf, cur_type);
  }
}

void CategoryStore::configureCommon(pStoreConf configuration,
                                    const string type) {
  // initialize model store
  modelStore = createStore(type, categoryHandled, false, false);
  LOG_OPER("[%s] %s: Configured store of type %s successfully.",
           categoryHandled.c_str(), getType().c_str(), type.c_str());
  modelStore->configure(configuration);
}

void CategoryStore::close() {
  for (map<string, shared_ptr<Store> >::iterator iter = stores.begin();
      iter != stores.end();
      ++iter) {
    iter->second->close();
  }
}

bool CategoryStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
  shared_ptr<logentry_vector_t> singleMessage(new logentry_vector_t);
  shared_ptr<logentry_vector_t> failed_messages(new logentry_vector_t);
  logentry_vector_t::iterator message_iter;

  for (message_iter = messages->begin();
      message_iter != messages->end();
      ++message_iter) {
    map<string, shared_ptr<Store> >::iterator store_iter;
    shared_ptr<Store> store;
    string category = (*message_iter)->category;

    store_iter = stores.find(category);

    if (store_iter == stores.end()) {
      // Create new store for this category
      store = modelStore->copy(category);
      store->open();
      stores[category] = store;
    } else {
      store = store_iter->second;
    }

    if (store == NULL || !store->isOpen()) {
      LOG_OPER("[%s] Failed to open store for category <%s>",
               categoryHandled.c_str(), category.c_str());
      failed_messages->push_back(*message_iter);
      continue;
    }

    // send this message to the store that handles this category
    singleMessage->clear();
    singleMessage->push_back(*message_iter);

    if (!store->handleMessages(singleMessage)) {
      LOG_OPER("[%s] Failed to handle message for category <%s>",
               categoryHandled.c_str(), category.c_str());
      failed_messages->push_back(*message_iter);
      continue;
    }
  }

  if (!failed_messages->empty()) {
    // Did not handle all messages, update message vector
    messages->swap(*failed_messages);
    return false;
  } else {
    return true;
  }
}

void CategoryStore::periodicCheck() {
  for (map<string, shared_ptr<Store> >::iterator iter = stores.begin();
      iter != stores.end();
      ++iter) {
    iter->second->periodicCheck();
  }
}

void CategoryStore::flush() {
  for (map<string, shared_ptr<Store> >::iterator iter = stores.begin();
      iter != stores.end();
      ++iter) {
    iter->second->flush();
  }
}

