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
#include "store_buffer.h"

using namespace std;
using namespace boost;

#define DEFAULT_BUFFERSTORE_MAX_QUEUE_LENGTH     2000000
#define DEFAULT_BUFFERSTORE_SEND_RATE            1
#define DEFAULT_BUFFERSTORE_AVG_RETRY_INTERVAL   300
#define DEFAULT_BUFFERSTORE_RETRY_INTERVAL_RANGE 60

BufferStore::BufferStore(const string& category, bool multi_category,
                         const string& trigger_path)
  : Store(category, "buffer", multi_category, trigger_path),
    maxQueueLength(DEFAULT_BUFFERSTORE_MAX_QUEUE_LENGTH),
    bufferSendRate(DEFAULT_BUFFERSTORE_SEND_RATE),
    avgRetryInterval(DEFAULT_BUFFERSTORE_AVG_RETRY_INTERVAL),
    retryIntervalRange(DEFAULT_BUFFERSTORE_RETRY_INTERVAL_RANGE),
    replayBuffer(true),
    state(DISCONNECTED) {

  lastWriteTime = lastOpenAttempt = time(NULL);
  retryInterval = getNewRetryInterval();

  // we can't open the client conection until we get configured
}

BufferStore::~BufferStore() {

}

void BufferStore::configure(pStoreConf configuration) {

  // Constructor defaults are fine if these don't exist
  configuration->getUnsigned("max_queue_length", (unsigned long&) maxQueueLength);
  configuration->getUnsigned("buffer_send_rate", (unsigned long&) bufferSendRate);
  configuration->getUnsigned("retry_interval", (unsigned long&) avgRetryInterval);
  configuration->getUnsigned("retry_interval_range", (unsigned long&) retryIntervalRange);

  string tmp;
  if (configuration->getString("replay_buffer", tmp) && tmp != "yes") {
    replayBuffer = false;
  }

  if (retryIntervalRange > avgRetryInterval) {
    LOG_OPER("[%s] Bad config - retry_interval_range must be less than retry_interval. Using <%d> as range instead of <%d>",
             categoryHandled.c_str(), (int)avgRetryInterval, (int)retryIntervalRange);
    retryIntervalRange = avgRetryInterval;
  }

  pStoreConf secondary_store_conf;
  if (!configuration->getStore("secondary", secondary_store_conf)) {
    string msg("Bad config - buffer store doesn't have secondary store");
    setStatus(msg);
    cout << msg << endl;
  } else {
    string type;
    if (!secondary_store_conf->getString("type", type)) {
      string msg("Bad config - buffer secondary store doesn't have a type");
      setStatus(msg);
      cout << msg << endl;
    } else {
      // If replayBuffer is true, then we need to create a readable store
      secondaryStore = createStore(type, categoryHandled, replayBuffer,
                                   multiCategory);
      secondaryStore->configure(secondary_store_conf);
    }
  }

  pStoreConf primary_store_conf;
  if (!configuration->getStore("primary", primary_store_conf)) {
    string msg("Bad config - buffer store doesn't have primary store");
    setStatus(msg);
    cout << msg << endl;
  } else {
    string type;
    if (!primary_store_conf->getString("type", type)) {
      string msg("Bad config - buffer primary store doesn't have a type");
      setStatus(msg);
      cout << msg << endl;
    } else if (0 == type.compare("multi")) {
      // Cannot allow multistores in bufferstores as they can partially fail to
      // handle a message.  We cannot retry sending a messages that was
      // already handled by a subset of stores in the multistore.
      string msg("Bad config - buffer primary store cannot be multistore");
      setStatus(msg);
    } else {
      primaryStore = createStore(type, categoryHandled, false, multiCategory);
      primaryStore->configure(primary_store_conf);
    }
  }

  // If the config is bad we'll still try to write the data to a
  // default location on local disk.
  if (!secondaryStore) {
    secondaryStore = createStore("file", categoryHandled, true, multiCategory);
  }
  if (!primaryStore) {
    primaryStore = createStore("file", categoryHandled, false, multiCategory);
  }
}

bool BufferStore::isOpen() {
  return primaryStore->isOpen() || secondaryStore->isOpen();
}

bool BufferStore::open() {

  // try to open the primary store, and set the state accordingly
  if (primaryStore->open()) {
    // in case there are files left over from a previous instance
    changeState(SENDING_BUFFER);

    // If we don't need to send buffers, skip to streaming
    if (!replayBuffer) {
      // We still switch state to SENDING_BUFFER first just to make sure we
      // can open the secondary store
      changeState(STREAMING);
    }
  } else {
    secondaryStore->open();
    changeState(DISCONNECTED);
  }

  return isOpen();
}

void BufferStore::close() {
  if (primaryStore->isOpen()) {
    primaryStore->flush();
    primaryStore->close();
  }
  if (secondaryStore->isOpen()) {
    secondaryStore->flush();
    secondaryStore->close();
  }
}

void BufferStore::flush() {
  if (primaryStore->isOpen()) {
    primaryStore->flush();
  }
  if (secondaryStore->isOpen()) {
    secondaryStore->flush();
  }
}

shared_ptr<Store> BufferStore::copy(const std::string &category) {
  BufferStore *store = new BufferStore(category, multiCategory, triggerPath);
  shared_ptr<Store> copied = shared_ptr<Store>(store);

  store->maxQueueLength = maxQueueLength;
  store->bufferSendRate = bufferSendRate;
  store->avgRetryInterval = avgRetryInterval;
  store->retryIntervalRange = retryIntervalRange;
  store->replayBuffer = replayBuffer;

  store->primaryStore = primaryStore->copy(category);
  store->secondaryStore = secondaryStore->copy(category);
  return copied;
}

bool BufferStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
  lastWriteTime = time(NULL);

  // If the queue is really long it's probably because the primary store isn't moving
  // fast enough and is backing up, in which case it's best to give up on it for now.
  if (state == STREAMING && messages->size() > maxQueueLength) {
    LOG_OPER("[%s] BufferStore queue backing up, switching to secondary store (%u messages)", categoryHandled.c_str(), (unsigned)messages->size());
    changeState(DISCONNECTED);
  }

  if (state == STREAMING) {
    if (primaryStore->handleMessages(messages)) {
      return true;
    } else {
      changeState(DISCONNECTED);
    }
  }

  if (state != STREAMING) {
    // If this fails there's nothing else we can do here.
    return secondaryStore->handleMessages(messages);
  }

  return false;
}

// handles entry and exit conditions for states
void BufferStore::changeState(buffer_state_t new_state) {

  // leaving this state
  switch (state) {
  case STREAMING:
    secondaryStore->open();
    break;
  case DISCONNECTED:
    // Assume that if we are now able to leave the disconnected state, any
    // former warning has now been fixed.
    setStatus("");
    break;
  case SENDING_BUFFER:
    break;
  default:
    break;
  }

  // entering this state
  switch (new_state) {
  case STREAMING:
    if (secondaryStore->isOpen()) {
      secondaryStore->close();
    }
    break;
  case DISCONNECTED:
    // Do not set status here as it is possible to be in this frequently.
    // Whatever caused us to enter this state should have either set status
    // or chosen not to set status.
    g_Handler->incrementCounter("retries");
    lastOpenAttempt = time(NULL);
    retryInterval = getNewRetryInterval();
    LOG_OPER("[%s] choosing new retry interval <%d> seconds", categoryHandled.c_str(),
             (int)retryInterval);
    if (!secondaryStore->isOpen()) {
      secondaryStore->open();
    }
    break;
  case SENDING_BUFFER:
    if (!secondaryStore->isOpen()) {
      secondaryStore->open();
    }
    break;
  default:
    break;
  }

  LOG_OPER("[%s] Changing state from <%s> to <%s>", categoryHandled.c_str(), stateAsString(state), stateAsString(new_state));
  state = new_state;
}

void BufferStore::periodicCheck() {

  // This class is responsible for checking its children
  primaryStore->periodicCheck();
  secondaryStore->periodicCheck();

  time_t now = time(NULL);
  struct tm nowinfo;
  localtime_r(&now, &nowinfo);

  if (state == DISCONNECTED) {

    if (now - lastOpenAttempt > retryInterval) {

      if (primaryStore->open()) {
        // Success.  Check if we need to send buffers from secondary to primary
        if (replayBuffer) {
          changeState(SENDING_BUFFER);
        } else {
          changeState(STREAMING);
        }
      } else {
        // this resets the retry timer
        changeState(DISCONNECTED);
      }
    }
  }

  if (state == SENDING_BUFFER) {

    // Read a group of messages from the secondary store and send them to
    // the primary store. Note that the primary store could tell us to try
    // again later, so this isn't very efficient if it reads too many
    // messages at once. (if the secondary store is a file, the number of
    // messages read is controlled by the max file size)
    unsigned sent = 0;
    for (sent = 0; sent < bufferSendRate; ++sent) {
      boost::shared_ptr<logentry_vector_t> messages(new logentry_vector_t);
      if (secondaryStore->readOldest(messages, &nowinfo)) {
        lastWriteTime = time(NULL);

        unsigned long size = messages->size();
        if (size) {
          if (primaryStore->handleMessages(messages)) {
            secondaryStore->deleteOldest(&nowinfo);
          } else {

            if (messages->size() != size) {
              // We were only able to process some, but not all of this batch
              // of messages.  Replace this batch of messages with just the messages
              // that were not processed.
              LOG_OPER("[%s] buffer store primary store processed %lu/%lu messages",
                       categoryHandled.c_str(), size - messages->size(), size);

              // Put back un-handled messages
              if (!secondaryStore->replaceOldest(messages, &nowinfo)) {
                // Nothing we can do but try to remove oldest messages and report a loss
                LOG_OPER("[%s] buffer store secondary store lost %lu messages",
                         categoryHandled.c_str(), messages->size());
                g_Handler->incrementCounter("lost", messages->size());
                secondaryStore->deleteOldest(&nowinfo);
              }
            }

            changeState(DISCONNECTED);
            break;
          }
        }  else {
          // else it's valid for read to not find anything but not error
          secondaryStore->deleteOldest(&nowinfo);
        }
      } else {
        // This is bad news. We'll stay in the sending state and keep trying to read.
        setStatus("Failed to read from secondary store");
        LOG_OPER("[%s] WARNING: buffer store can't read from secondary store", categoryHandled.c_str());
        break;
      }

      if (secondaryStore->empty(&nowinfo)) {
        LOG_OPER("[%s] No more buffer files to send, switching to streaming mode", categoryHandled.c_str());
        changeState(STREAMING);

        primaryStore->flush();
        break;
      }
    }
  }// if state == SENDING_BUFFER
}


time_t BufferStore::getNewRetryInterval() {
  time_t interval = avgRetryInterval - retryIntervalRange/2 + rand() % retryIntervalRange;
  return interval;
}

const char* BufferStore::stateAsString(buffer_state_t state) {
switch (state) {
  case STREAMING:
    return "STREAMING";
  case DISCONNECTED:
    return "DISCONNECTED";
  case SENDING_BUFFER:
    return "SENDING_BUFFER";
  default:
    return "unknown state";
  }
}

std::string BufferStore::getStatus() {

  // This order is intended to give precedence to the errors
  // that are likely to be the worst. We can handle a problem
  // with the primary store, but not the secondary.
  std::string return_status = secondaryStore->getStatus();
  if (return_status.empty()) {
    return_status = Store::getStatus();
  }
  if (return_status.empty()) {
    return_status = primaryStore->getStatus();
  }
  return return_status;
}

