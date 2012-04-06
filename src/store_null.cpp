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
#include "store_null.h"

using namespace std;
using namespace boost;

NullStore::NullStore(const std::string& category, bool multi_category,
                     const string& trigger_path)
  : Store(category, "null", multi_category, trigger_path)
{}

NullStore::~NullStore() {
}

boost::shared_ptr<Store> NullStore::copy(const std::string &category) {
  NullStore *store = new NullStore(category, multiCategory, triggerPath);
  shared_ptr<Store> copied = shared_ptr<Store>(store);
  return copied;
}

bool NullStore::open() {
  return true;
}

bool NullStore::isOpen() {
  return true;
}

void NullStore::configure(pStoreConf) {
}

void NullStore::close() {
}

bool NullStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
  g_Handler->incrementCounter("ignored", messages->size());
  return true;
}

void NullStore::flush() {
}

bool NullStore::readOldest(/*out*/ boost::shared_ptr<logentry_vector_t> messages,
                       struct tm* now) {
  return true;
}

bool NullStore::replaceOldest(boost::shared_ptr<logentry_vector_t> messages,
                              struct tm* now) {
  return true;
}

void NullStore::deleteOldest(struct tm* now) {
}

bool NullStore::empty(struct tm* now) {
  return true;
}

