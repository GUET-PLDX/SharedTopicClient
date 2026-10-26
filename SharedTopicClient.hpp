#pragma once

// clang-format off
/* === MODULE MANIFEST V2 ===
module_description: SharedTopicClient 是一个多 Topic 数据共享与串口转发客户端模块。它用于通过 UART 将多个 Topic
  的数据统一打包、发送，实现消息流的串口透明同步转发，适用于分布式系统的多主题数据同步或边缘数据采集。 / SharedTopicClient is a client
  module for multi-topic data sharing and transparent UART forwarding. It subscribes
  to multiple Topics, packs their updates, and transmits them via UART, enabling efficient
  and reliable message synchronization over serial connections—ideal for distributed
  systems or edge data acquisition.
depends: []
=== END MANIFEST === */
// clang-format on

#include <atomic>
#include <cstddef>
#include <cstdint>

#include "flag.hpp"
#include "libxr.hpp"
#include "message.hpp"
#include "queue/mpmc_queue.hpp"
#include "uart.hpp"

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

class SharedTopicClient {
 private:
  struct CallbackInfo {
    SharedTopicClient* client;
    LibXR::Topic::TopicHandle topic;
  };

  struct PacketSlot {
    LibXR::RawData buffer;
  };

  struct ReadyPacket {
    uint32_t slot_index = 0;
    size_t packet_size = 0;
  };

 public:
  struct TopicConfig {
    const char* name;
    const char* domain = "libxr_def_domain";

    TopicConfig(const char* name) : name(name) {}

    TopicConfig(const char* name, const char* domain)
        : name(name), domain(domain) {}
  };

  SharedTopicClient(LibXR::UART& uart, uint32_t slot_count = 16,
                    std::initializer_list<TopicConfig> topic_configs =
                        {"topic1", {"topic2", "libxr_def_domain"}})
      : uart_(&uart) {
    ASSERT(uart_->write_port_ != nullptr);
    ASSERT(uart_->write_port_->Capacity() > 0);
    ASSERT(uart_->write_port_->Writable());
    ASSERT(topic_configs.size() > 0);
    ASSERT(slot_count > 0);

    size_t max_packet_size = 0;

    for (auto config : topic_configs) {
      auto domain = LibXR::Topic::Domain(config.domain);
      auto topic = LibXR::Topic::Find(config.name, &domain);
      if (topic == nullptr) {
        XR_LOG_ERROR("Topic not found: %s/%s", config.domain, config.name);
        libxr_fatal_error(__FILE__, __LINE__, false);
      }
      const size_t PACKET_SIZE =
          topic->data_.payload_size + LibXR::Topic::PACK_BASE_SIZE;
      max_packet_size = LibXR::max(max_packet_size, PACKET_SIZE);
    }

    ASSERT(max_packet_size <= uart_->write_port_->Capacity());

    const size_t QUEUE_CAPACITY =
        LibXR::max(static_cast<size_t>(slot_count), size_t{2});
    packets_ = new PacketSlot[slot_count];
    // One spare queue cell covers a preempted Pop before it releases its cell.
    free_slots_ = new LibXR::MPMCQueue<uint32_t>(QUEUE_CAPACITY + 1);
    ready_packets_ = new LibXR::MPMCQueue<ReadyPacket>(QUEUE_CAPACITY);
    for (uint32_t i = 0; i < slot_count; i++) {
      packets_[i].buffer =
          LibXR::RawData(new uint8_t[max_packet_size], max_packet_size);
      ASSERT(free_slots_->Push(i) == LibXR::ErrorCode::OK);
    }

    tx_callback_ = LibXR::Callback<LibXR::ErrorCode>::CreateGuarded(
        [](bool in_isr, SharedTopicClient* self, LibXR::ErrorCode status) {
          self->OnWriteDone(in_isr, status);
        },
        this);
    tx_op_ = LibXR::WriteOperation(tx_callback_);

    for (auto config : topic_configs) {
      auto domain = LibXR::Topic::Domain(config.domain);
      auto topic_handle = LibXR::Topic::Find(config.name, &domain);
      ASSERT(topic_handle != nullptr);
      void (*func)(bool, CallbackInfo, const LibXR::Topic::RawMessageView&) =
          [](bool in_isr, CallbackInfo info,
             const LibXR::Topic::RawMessageView& message) {
            info.client->OnTopic(in_isr, info, message);
          };

      auto msg_cb = LibXR::Topic::Callback::Create(
          func, CallbackInfo{this, topic_handle});

      LibXR::Topic topic(topic_handle);

      topic.RegisterCallback(msg_cb);
    }
  }

  void OnMonitor() {}

 private:
  void OnTopic(bool in_isr, CallbackInfo info,
               const LibXR::Topic::RawMessageView& message) {
    // Only one free-slot consumer may be preempted; the spare queue cell
    // covers its unpublished Pop. Concurrent/reentrant updates are dropped.
    if (topic_dispatch_running_.TestAndSet()) {
      return;
    }

    ProcessTopic(in_isr, info, message);
    topic_dispatch_running_.Clear();
  }

  void ProcessTopic(bool in_isr, CallbackInfo info,
                    const LibXR::Topic::RawMessageView& message) {
    const size_t packet_size =
        message.payload.size_ + LibXR::Topic::PACK_BASE_SIZE;
    uint32_t slot_index = 0;

    if (free_slots_->Pop(slot_index) != LibXR::ErrorCode::OK) {
      return;
    }

    auto& slot = packets_[slot_index];
    ASSERT(packet_size <= slot.buffer.size_);
    if (LibXR::Topic(info.topic)
            .PackRaw(message.payload, slot.buffer, message.timestamp) !=
        LibXR::ErrorCode::OK) {
      ReturnFreeSlot(slot_index);
      return;
    }

    if (ready_packets_->Push(ReadyPacket{slot_index, packet_size}) !=
        LibXR::ErrorCode::OK) {
      ReturnFreeSlot(slot_index);
      return;
    }
    RequestTx(in_isr);
  }

  void RequestTx(bool in_isr) {
    tx_gate_.Request([&] { TxService(in_isr); });
  }

  void TxService(bool in_isr) {
    ReadyPacket packet;
    if (ready_packets_->Pop(packet) != LibXR::ErrorCode::OK) {
      return;
    }

    auto& slot = packets_[packet.slot_index];
    tx_gate_.SetActive(packet.slot_index);
    auto write_status =
        uart_->Write(LibXR::ConstRawData{slot.buffer.addr_, packet.packet_size},
                     tx_op_, in_isr);
    if (static_cast<int8_t>(write_status) < 0) {
      const uint32_t active_slot = tx_gate_.Complete();
      if (active_slot != SharedTopicTxGate::INVALID_SLOT) {
        ReturnFreeSlot(active_slot);
      }
      RequestTx(in_isr);
      return;
    }
  }

  void OnWriteDone(bool in_isr, LibXR::ErrorCode status) {
    const uint32_t active_slot = tx_gate_.Complete();
    if (active_slot != SharedTopicTxGate::INVALID_SLOT) {
      ReturnFreeSlot(active_slot);
    }
    UNUSED(status);
    RequestTx(in_isr);
  }

  void ReturnFreeSlot(uint32_t slot_index) {
    ASSERT(free_slots_->Push(slot_index) == LibXR::ErrorCode::OK);
  }

  LibXR::UART* uart_;
  PacketSlot* packets_ = nullptr;
  LibXR::MPMCQueue<uint32_t>* free_slots_ = nullptr;
  LibXR::MPMCQueue<ReadyPacket>* ready_packets_ = nullptr;
  LibXR::Callback<LibXR::ErrorCode> tx_callback_;
  LibXR::WriteOperation tx_op_;
  SharedTopicTxGate tx_gate_;
  LibXR::Flag::Atomic topic_dispatch_running_;
};
