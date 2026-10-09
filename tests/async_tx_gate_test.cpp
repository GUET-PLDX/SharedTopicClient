#include <cassert>

#include "../SharedTopicTxGate.hpp"

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
}
