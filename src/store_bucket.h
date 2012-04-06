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

#ifndef SCRIBE_STORE_BUCKET_H
#define SCRIBE_STORE_BUCKET_H


#include "common.h" // includes std libs, thrift, and stl typedefs
#include "conf.h"
#include "file.h"
#include "conn_pool.h"

/*
 * This store separates messages into many groups based on a
 * hash function, and sends each group to a different store.
 */
class BucketStore : public Store {

 public:
  BucketStore(const std::string& category, bool multi_category,
              const std::string& trigger_path);
  ~BucketStore();

  boost::shared_ptr<Store> copy(const std::string &category);
  bool handleMessages(boost::shared_ptr<logentry_vector_t> messages);
  bool open();
  bool isOpen();
  void configure(pStoreConf configuration);
  void close();
  void flush();
  void periodicCheck();

  std::string getStatus();

 protected:
  enum bucketizer_type {
    context_log,
    random,      // randomly hash messages without using any key
    key_hash,    // use hashing to split keys into buckets
    key_modulo,  // use modulo to split keys into buckets
    key_range    // use bucketRange to compute modulo to split keys into buckets
  };

  bucketizer_type bucketType;
  char delimiter;
  bool removeKey;
  bool opened;
  unsigned long bucketRange;  // used to compute key_range bucketizing
  unsigned long numBuckets;
  std::vector<boost::shared_ptr<Store> > buckets;

  unsigned long bucketize(const std::string& message);
  std::string getMessageWithoutKey(const std::string& message);

 private:
  // disallow copy, assignment, and emtpy construction
  BucketStore();
  BucketStore(BucketStore& rhs);
  BucketStore& operator=(BucketStore& rhs);
  void createBucketsFromBucket(pStoreConf configuration,
                               pStoreConf bucket_conf);
  void createBuckets(pStoreConf configuration);
};

#endif // SCRIBE_STORE_BUCKET_H

