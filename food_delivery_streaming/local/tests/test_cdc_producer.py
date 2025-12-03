#!/usr/bin/env python3
"""
Test suite for CDC Producer
Assignment 2 - Data Stores & Pipelines
Student: Anik Das (2025EM1100026)

Note: These are my first unit tests, so they might not be perfect!
I tried to test the main functionality that could break.
"""

import unittest
import json
import tempfile
import os
from datetime import datetime
from producers.orders_cdc_producer import create_json_event, get_last_processed_timestamp, save_last_processed_timestamp

class TestCDCProducer(unittest.TestCase):
    """Test cases for CDC Producer functionality"""

    def setUp(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.timestamp_file = os.path.join(self.temp_dir, "test_timestamp.txt")

        # Mock row object for testing
        class MockRow:
            def __init__(self):
                self.order_id = 101
                self.customer_name = "Test Customer"
                self.restaurant_name = "Test Restaurant"
                self.item = "Test Item"
                self.amount = 250.50
                self.order_status = "PLACED"
                self.created_at = datetime(2025, 11, 18, 12, 24, 0)

        self.mock_row = MockRow()

    def tearDown(self):
        """Clean up test environment"""
        os.remove(self.timestamp_file)
        os.rmdir(self.temp_dir)

    def test_json_event_creation(self):
        """Test JSON event creation from row data"""
        json_event = create_json_event(self.mock_row)

        expected = {
            "order_id": 101,
            "customer_name": "Test Customer",
            "restaurant_name": "Test Restaurant",
            "item": "Test Item",
            "amount": 250.5,
            "order_status": "PLACED",
            "created_at": "2025-11-18T12:24:00Z"
        }

        self.assertEqual(json_event, expected)
        # TODO: Maybe add more assertions for edge cases?

    def test_timestamp_file_handling(self):
        """Test timestamp file read/write operations"""
        # Test initial read (file doesn't exist)
        initial_timestamp = get_last_processed_timestamp(self.timestamp_file)
        self.assertEqual(initial_timestamp, "1970-01-01 00:00:00")

        # Test write operation
        test_timestamp = "2025-11-18 12:24:00"
        save_last_processed_timestamp(self.timestamp_file, test_timestamp)

        # Test read after write
        saved_timestamp = get_last_processed_timestamp(self.timestamp_file)
        self.assertEqual(saved_timestamp, test_timestamp)
        # This seems to work, but I'm not sure if I'm testing all edge cases...

    def test_json_validation(self):
        """Test JSON validation for required fields"""
        json_event = create_json_event(self.mock_row)
        json_str = json.dumps(json_event)

        # Verify JSON is valid
        parsed = json.loads(json_str)
        self.assertEqual(parsed["order_id"], 101)
        self.assertEqual(parsed["amount"], 250.5)
        self.assertEqual(parsed["order_status"], "PLACED")

        # Verify required fields exist
        required_fields = ["order_id", "customer_name", "restaurant_name", "item", "amount", "order_status", "created_at"]
        for field in required_fields:
            self.assertIn(field, parsed)

    def test_edge_cases(self):
        """Test edge cases and error conditions"""
        # Test with None values - this was tricky to figure out!
        class MockRowWithNone:
            def __init__(self):
                self.order_id = None
                self.customer_name = None
                self.restaurant_name = "Test"
                self.item = "Test"
                self.amount = -10.0  # Negative amount
                self.order_status = "INVALID"  # Invalid status
                self.created_at = None

        mock_row_none = MockRowWithNone()
        json_event = create_json_event(mock_row_none)

        # Should still create JSON even with problematic data
        self.assertIsNotNone(json_event)
        self.assertEqual(json_event["order_id"], None)
        self.assertEqual(json_event["amount"], -10.0)
        # Not sure if this is the right way to handle nulls, but it works for now

    def test_timestamp_edge_cases(self):
        """Test timestamp edge cases"""
        class MockRowWithEdgeTimestamp:
            def __init__(self):
                self.order_id = 102
                self.customer_name = "Edge Case User"
                self.restaurant_name = "Edge Restaurant"
                self.item = "Edge Item"
                self.amount = 999.99
                self.order_status = "DELIVERED"
                # Test with very old timestamp
                self.created_at = datetime(2000, 1, 1, 0, 0, 0)

        mock_row_edge = MockRowWithEdgeTimestamp()
        json_event = create_json_event(mock_row_edge)

        self.assertEqual(json_event["created_at"], "2000-01-01T00:00:00Z")
        self.assertEqual(json_event["amount"], 999.99)

    def test_special_characters(self):
        """Test handling of special characters in data"""
        class MockRowWithSpecialChars:
            def __init__(self):
                self.order_id = 103
                self.customer_name = "O'Brien's Test"
                self.restaurant_name = "McDonald's & Pizza"
                self.item = "Special \"Item\" with 'quotes'"
                self.amount = 123.45
                self.order_status = "PLACED"
                self.created_at = datetime(2025, 12, 25, 12, 30, 0)

        mock_row_special = MockRowWithSpecialChars()
        json_event = create_json_event(mock_row_special)

        self.assertEqual(json_event["order_id"], 103)
        self.assertEqual(json_event["amount"], 123.45)
        self.assertIn("O'Brien", json_event["customer_name"])

if __name__ == "__main__":
    unittest.main()
    # Note: I should probably add more tests, but this covers the main functionality