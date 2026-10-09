#pragma once

#include <cstdint>

#include "flag.hpp"

class SharedTopicTxGate {
 public:
  static constexpr uint32_t INVALID_SLOT = UINT32_MAX;

  template <typename Send>
  void Request(Send send) {
    pending_.store(true, std::memory_order_release);
    if (running_.TestAndSet()) {
      return;
    }

    do {
      pending_.store(false, std::memory_order_release);
      if (active_slot_.load(std::memory_order_acquire) == INVALID_SLOT) {
        send();
      }
    } while (pending_.load(std::memory_order_acquire) &&
             active_slot_.load(std::memory_order_acquire) == INVALID_SLOT);

    running_.Clear();
    if (pending_.load(std::memory_order_acquire) &&
        active_slot_.load(std::memory_order_acquire) == INVALID_SLOT) {
      Request(send);
    }
  }

  void SetActive(uint32_t slot) {
    active_slot_.store(slot, std::memory_order_release);
  }

  uint32_t Complete() {
    return active_slot_.exchange(INVALID_SLOT, std::memory_order_acq_rel);
  }

 private:
  std::atomic<uint32_t> active_slot_{INVALID_SLOT};
  LibXR::Flag::Atomic running_;
  std::atomic_bool pending_{false};
};
