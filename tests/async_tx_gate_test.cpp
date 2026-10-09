#include <cassert>

#include "../SharedTopicClient.hpp"

int main() {
  SharedTopicTxGate gate;
  int sends = 0;
  gate.Request([&] {
    ++sends;
    gate.SetActive(1U);
  });
  assert(sends == 1);
  assert(gate.Complete() == 1U);

  gate.Request([&] {
    ++sends;
    gate.SetActive(2U);
  });
  assert(sends == 2);
  assert(gate.Complete() == 2U);
  assert(gate.Complete() == SharedTopicTxGate::INVALID_SLOT);

  // A request while a write is active must not submit another write.
  gate.SetActive(3U);
  gate.Request([&] { ++sends; });
  assert(sends == 2);
  assert(gate.Complete() == 3U);

  // Synchronous completions request more work without recursively sending.
  int depth = 0;
  int remaining = 10000;
  auto send = [&] {
    ++depth;
    assert(depth == 1);
    gate.SetActive(4U);
    assert(gate.Complete() == 4U);
    if (--remaining > 0) {
      gate.Request([&] { assert(false); });
    }
    --depth;
  };
  gate.Request(send);
  assert(remaining == 0);
}
