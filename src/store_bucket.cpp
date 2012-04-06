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
#include "store.h"
#include "store_bucket.h"

using namespace std;
using namespace boost;
using namespace scribe::thrift;

#define DEFAULT_BUCKETSTORE_DELIMITER            ':'

BucketStore::BucketStore(const string& category, bool multi_category,
                         const string& trigger_path)
  : Store(category, "bucket", multi_category, trigger_path),
    bucketType(context_log),
    delimiter(DEFAULT_BUCKETSTORE_DELIMITER),
    removeKey(false),
    opened(false),
    bucketRange(0),
    numBuckets(1) {
}

BucketStore::~BucketStore() {

}

// Given a single bucket definition, create multiple buckets
void BucketStore::createBucketsFromBucket(pStoreConf configuration,
					  pStoreConf bucket_conf) {
  string error_msg, bucket_subdir, type, path, failure_bucket;
  bool needs_bucket_subdir = false;
  unsigned long bucket_offset = 0;
  pStoreConf tmp;

  // check for extra bucket definitions
  if (configuration->getStore("bucket0", tmp) ||
      configuration->getStore("bucket1", tmp)) {
    error_msg = "bucket store has too many buckets defined";
    goto handle_error;
  }

  bucket_conf->getString("type", type);
  if (type != "file" && type != "thriftfile") {
    error_msg = "store contained in a bucket store must have a type of ";
    error_msg += "either file or thriftfile if not defined explicitely";
    goto handle_error;
  }

  needs_bucket_subdir = true;
  if (!configuration->getString("bucket_subdir", bucket_subdir)) {
    error_msg =
      "bucketizer containing file stores must have a bucket_subdir";
    goto handle_error;
  }
  if (!bucket_conf->getString("file_path", path)) {
    error_msg =
      "file store contained by bucketizer must have a file_path";
    goto handle_error;
  }

  // set starting bucket number if specified
  configuration->getUnsigned("bucket_offset", bucket_offset);

  // check if failure bucket was given a different name
  configuration->getString("failure_bucket", failure_bucket);

  // We actually create numBuckets + 1 stores. Messages are normally
  // hashed into buckets 1 through numBuckets, and messages that can't
  // be hashed are put in bucket 0.

  for (unsigned int i = 0; i <= numBuckets; ++i) {

    shared_ptr<Store> newstore =
      createStore(type, categoryHandled, false, multiCategory);

    if (!newstore) {
      error_msg = "can't create store of type: ";
      error_msg += type;
      goto handle_error;
    }

    // For file/thrift file buckets, create unique filepath for each bucket
    if (needs_bucket_subdir) {
      if (i == 0 && !failure_bucket.empty()) {
        bucket_conf->setString("file_path", path + '/' + failure_bucket);
      } else {
        // the bucket number is appended to the file path
        unsigned int bucket_id = i + bucket_offset;

        ostringstream oss;
        oss << path << '/' << bucket_subdir << setw(3) << setfill('0')
            << bucket_id;
        bucket_conf->setString("file_path", oss.str());
      }
    }

    buckets.push_back(newstore);
    newstore->configure(bucket_conf);
  }

  return;

handle_error:
  setStatus(error_msg);
  LOG_OPER("[%s] Bad config - %s", categoryHandled.c_str(),
           error_msg.c_str());
  numBuckets = 0;
  buckets.clear();
}

// Checks for a bucket definition for every bucket from 0 to numBuckets
// and configures each bucket
void BucketStore::createBuckets(pStoreConf configuration) {
  string error_msg, tmp_string;
  pStoreConf tmp;
  unsigned long i;

  if (configuration->getString("bucket_subdir", tmp_string)) {
    error_msg =
      "cannot have bucket_subdir when defining multiple buckets";
      goto handle_error;
  }

  if (configuration->getString("bucket_offset", tmp_string)) {
    error_msg =
      "cannot have bucket_offset when defining multiple buckets";
      goto handle_error;
  }

  if (configuration->getString("failure_bucket", tmp_string)) {
    error_msg =
      "cannot have failure_bucket when defining multiple buckets";
      goto handle_error;
  }

  // Configure stores named 'bucket0, bucket1, bucket2, ... bucket{numBuckets}
  for (i = 0; i <= numBuckets; i++) {
    pStoreConf   bucket_conf;
    string       type, bucket_name;
    stringstream ss;

    ss << "bucket" << i;
    bucket_name = ss.str();

    if (!configuration->getStore(bucket_name, bucket_conf)) {
      error_msg = "could not find bucket definition for " +
	bucket_name;
      goto handle_error;
    }

    if (!bucket_conf->getString("type", type)) {
      error_msg =
	"store contained in a bucket store must have a type";
      goto handle_error;
    }

    shared_ptr<Store> bucket =
      createStore(type, categoryHandled, false, multiCategory);

    buckets.push_back(bucket);
    bucket->configure(bucket_conf);
  }

  // Check if an extra bucket is defined
  if (configuration->getStore("bucket" + (numBuckets + 1), tmp)) {
    error_msg = "bucket store has too many buckets defined";
    goto handle_error;
  }

  return;

handle_error:
  setStatus(error_msg);
  LOG_OPER("[%s] Bad config - %s", categoryHandled.c_str(),
           error_msg.c_str());
  numBuckets = 0;
  buckets.clear();
}

/**
   * Buckets in a bucket store can be defined explicitly or implicitly:
   *
   * #Explicitly
   * <store>
   *   type=bucket
   *   num_buckets=2
   *   bucket_type=key_hash
   *
   *   <bucket0>
   *     ...
   *   </bucket0>
   *
   *   <bucket1>
   *     ...
   *   </bucket1>
   *
   *   <bucket2>
   *     ...
   *   </bucket2>
   * </store>
   *
   * #Implicitly
   * <store>
   *   type=bucket
   *   num_buckets=2
   *   bucket_type=key_hash
   *
   *   <bucket>
   *     ...
   *   </bucket>
   * </store>
   */
void BucketStore::configure(pStoreConf configuration) {

  string error_msg, bucketizer_str, remove_key_str;
  unsigned long delim_long = 0;
  pStoreConf bucket_conf;
  //set this to true for bucket types that have a delimiter
  bool need_delimiter = false;

  configuration->getString("bucket_type", bucketizer_str);

  // Figure out th bucket type from the bucketizer string
  if (0 == bucketizer_str.compare("context_log")) {
    bucketType = context_log;
  } else if (0 == bucketizer_str.compare("random")) {
      bucketType = random;
  } else if (0 == bucketizer_str.compare("key_hash")) {
    bucketType = key_hash;
    need_delimiter = true;
  } else if (0 == bucketizer_str.compare("key_modulo")) {
    bucketType = key_modulo;
    need_delimiter = true;
  } else if (0 == bucketizer_str.compare("key_range")) {
    bucketType = key_range;
    need_delimiter = true;
    configuration->getUnsigned("bucket_range", bucketRange);

    if (bucketRange == 0) {
      LOG_OPER("[%s] config warning - bucket_range is 0",
               categoryHandled.c_str());
    }
  }

  // This is either a key_hash or key_modulo, not context log, figure out the delimiter and store it
  if (need_delimiter) {
    configuration->getUnsigned("delimiter", delim_long);
    if (delim_long > 255) {
      LOG_OPER("[%s] config warning - delimiter is too large to fit in a char, using default", categoryHandled.c_str());
      delimiter = DEFAULT_BUCKETSTORE_DELIMITER;
    } else if (delim_long == 0) {
      LOG_OPER("[%s] config warning - delimiter is zero, using default", categoryHandled.c_str());
      delimiter = DEFAULT_BUCKETSTORE_DELIMITER;
    } else {
      delimiter = (char)delim_long;
    }
  }

  // Optionally remove the key and delimiter of each message before bucketizing
  configuration->getString("remove_key", remove_key_str);
  if (remove_key_str == "yes") {
    removeKey = true;

    if (bucketType == context_log) {
      error_msg =
        "Bad config - bucketizer store of type context_log do not support remove_key";
      goto handle_error;
    }
  }

  if (!configuration->getUnsigned("num_buckets", numBuckets)) {
    error_msg = "Bad config - bucket store must have num_buckets";
    goto handle_error;
  }

  // Buckets can be defined explicitely or by specifying a single "bucket"
  if (configuration->getStore("bucket", bucket_conf)) {
    createBucketsFromBucket(configuration, bucket_conf);
  } else {
    createBuckets(configuration);
  }

  return;

handle_error:
  setStatus(error_msg);
  LOG_OPER("[%s] %s", categoryHandled.c_str(), error_msg.c_str());
  numBuckets = 0;
  buckets.clear();
}

bool BucketStore::open() {
  // we have one extra bucket for messages we can't hash
  if (numBuckets <= 0 || buckets.size() != numBuckets + 1) {
    LOG_OPER("[%s] Can't open bucket store with <%d> of <%lu> buckets", categoryHandled.c_str(), (int)buckets.size(), numBuckets);
    return false;
  }

  for (std::vector<shared_ptr<Store> >::iterator iter = buckets.begin();
       iter != buckets.end();
       ++iter) {

    if (!(*iter)->open()) {
      close();
      opened = false;
      return false;
    }
  }
  opened = true;
  return true;
}

bool BucketStore::isOpen() {
  return opened;
}

void BucketStore::close() {
  // don't check opened, because we can call this when some, but
  // not all, contained stores are opened. Calling close on a contained
  // store that's already closed shouldn't hurt anything.
  for (std::vector<shared_ptr<Store> >::iterator iter = buckets.begin();
       iter != buckets.end();
       ++iter) {
    (*iter)->close();
  }
  opened = false;
}

void BucketStore::flush() {
  for (std::vector<shared_ptr<Store> >::iterator iter = buckets.begin();
       iter != buckets.end();
       ++iter) {
    (*iter)->flush();
  }
}

string BucketStore::getStatus() {

  string retval = Store::getStatus();

  std::vector<shared_ptr<Store> >::iterator iter = buckets.begin();
  while (retval.empty() && iter != buckets.end()) {
    retval = (*iter)->getStatus();
    ++iter;
  }
  return retval;
}

void BucketStore::periodicCheck() {
  for (std::vector<shared_ptr<Store> >::iterator iter = buckets.begin();
       iter != buckets.end();
       ++iter) {
    (*iter)->periodicCheck();
  }
}

shared_ptr<Store> BucketStore::copy(const std::string &category) {
  BucketStore *store = new BucketStore(category, multiCategory, triggerPath);
  shared_ptr<Store> copied = shared_ptr<Store>(store);

  store->numBuckets = numBuckets;
  store->bucketType = bucketType;
  store->delimiter = delimiter;

  for (std::vector<shared_ptr<Store> >::iterator iter = buckets.begin();
       iter != buckets.end();
       ++iter) {
    store->buckets.push_back((*iter)->copy(category));
  }

  return copied;
}

bool BucketStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
  bool success = true;

  boost::shared_ptr<logentry_vector_t> failed_messages(new logentry_vector_t);
  vector<shared_ptr<logentry_vector_t> > bucketed_messages;
  bucketed_messages.resize(numBuckets + 1);

  if (numBuckets == 0) {
    LOG_OPER("[%s] Failed to write - no buckets configured",
             categoryHandled.c_str());
    setStatus("Failed write to bucket store");
    return false;
  }

  // batch messages by bucket
  for (logentry_vector_t::iterator iter = messages->begin();
       iter != messages->end();
       ++iter) {
    unsigned bucket = bucketize((*iter)->message);

    if (!bucketed_messages[bucket]) {
      bucketed_messages[bucket] =
        shared_ptr<logentry_vector_t> (new logentry_vector_t);
    }

    bucketed_messages[bucket]->push_back(*iter);
  }

  // handle all batches of messages
  for (unsigned long i = 0; i <= numBuckets; i++) {
    shared_ptr<logentry_vector_t> batch = bucketed_messages[i];

    if (batch) {

      if (removeKey) {
        // Create new set of messages with keys removed
        shared_ptr<logentry_vector_t> key_removed =
          shared_ptr<logentry_vector_t> (new logentry_vector_t);

        for (logentry_vector_t::iterator iter = batch->begin();
             iter != batch->end();
             ++iter) {
          logentry_ptr_t entry = logentry_ptr_t(new LogEntry);
          entry->category = (*iter)->category;
          entry->message = getMessageWithoutKey((*iter)->message);
          key_removed->push_back(entry);
        }
        batch = key_removed;
      }

      if (!buckets[i]->handleMessages(batch)) {
        // keep track of messages that were not handled
        failed_messages->insert(failed_messages->end(),
                                bucketed_messages[i]->begin(),
                                bucketed_messages[i]->end());
        success = false;
      }
    }
  }

  if (!success) {
    // return failed logentrys in messages
    messages->swap(*failed_messages);
  }

  return success;
}

unsigned long BucketStore::bucketize(const std::string& message) {

  string::size_type length = message.length();

  if (bucketType == context_log) {
    // the key is in ascii after the third delimiter
    char delim = 1;
    string::size_type pos = 0;
    for (int i = 0; i < 3; ++i) {
      pos = message.find(delim, pos);
      if (pos == string::npos || length <= pos + 1) {
        return 0;
      }
      ++pos;
    }
    if (message[pos] == delim) {
      return 0;
    }

    uint32_t id = strtoul(message.substr(pos).c_str(), NULL, 10);
    if (id == 0) {
      return 0;
    }

    if (numBuckets == 0) {
      return 0;
    } else {
      return (integerhash::hash32(id) % numBuckets) + 1;
    }
  } else if (bucketType == random) {
    // return any random bucket
    return (rand() % numBuckets) + 1;
  } else {
    // just hash everything before the first user-defined delimiter
    string::size_type pos = message.find(delimiter);
    if (pos == string::npos) {
      // if no delimiter found, write to bucket 0
      return 0;
    }

    string key = message.substr(0, pos).c_str();
    if (key.empty()) {
      // if no key found, write to bucket 0
      return 0;
    }

    if (numBuckets == 0) {
      return 0;
    } else {
      switch (bucketType) {
        case key_modulo:
          // No hashing, just simple modulo
          return (atol(key.c_str()) % numBuckets) + 1;
          break;
        case key_range:
          if (bucketRange == 0) {
            return 0;
          } else {
            // Calculate what bucket this key would fall into if we used
            // bucket_range to compute the modulo
           double key_mod = atol(key.c_str()) % bucketRange;
           return (unsigned long) ((key_mod / bucketRange) * numBuckets) + 1;
          }
          break;
        case key_hash:
        default:
          // Hashing by default.
          return (strhash::hash32(key.c_str()) % numBuckets) + 1;
          break;
      }
    }
  }

  return 0;
}

string BucketStore::getMessageWithoutKey(const std::string& message) {
  string::size_type pos = message.find(delimiter);

  if (pos == string::npos) {
    return message;
  }

  return message.substr(pos+1);
}

