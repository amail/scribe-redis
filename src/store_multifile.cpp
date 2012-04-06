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
#include "store_multifile.h"

using namespace std;
using namespace boost;

MultiFileStore::MultiFileStore(const std::string& category, bool multi_category,
                               const string& trigger_path)
  : CategoryStore(category, "MultiFileStore", multi_category, trigger_path) {
}

MultiFileStore::~MultiFileStore() {
}

void MultiFileStore::configure(pStoreConf configuration) {
  configureCommon(configuration, "file");
}

