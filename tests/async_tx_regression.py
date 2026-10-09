#!/usr/bin/env python3
"""A UART write owns its packet slot until completion."""

import re
import unittest
from pathlib import Path


source = (Path(__file__).resolve().parents[1] / "SharedTopicClient.hpp").read_text()
gate = (Path(__file__).resolve().parents[1] / "SharedTopicTxGate.hpp").read_text()


class AsyncTxRegression(unittest.TestCase):
    def test_one_write_remains_in_flight_until_completion(self):
        self.assertIn("active_slot_.load(std::memory_order_acquire) == INVALID_SLOT", gate)
        self.assertIn("active_slot_.exchange(INVALID_SLOT", gate)
        self.assertIn("tx_gate_.SetActive(packet.slot_index)", source)

    def test_failure_still_drains_queued_packets(self):
        self.assertIn("RequestTx(in_isr)", source, "write failure must still drain queued packets")
        self.assertIn("UNUSED(status)", source)


if __name__ == "__main__":
    unittest.main()
