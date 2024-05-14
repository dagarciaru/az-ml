from unittest import TestCase
from utils.dates import get_year_windows


class TestGetYearWindows(TestCase):

    def test_get_year_windows_one_window(self):
        expected_dates = [
            {'initDate': '1970-01-01', 'endDate': '2000-01-01'},
        ]
        windows = get_year_windows('1970-01-01', '2000-01-01', 30)

        self.assertEqual(windows, expected_dates)


    def test_get_year_windows_disparete_windows(self):
        expected_dates = [
            {'initDate': '1970-01-01', 'endDate': '2000-01-01'},
            {'initDate': '2000-01-02', 'endDate': '2000-01-04'},
        ]
        windows = get_year_windows('1970-01-01', '2000-01-04', 30)

        self.assertEqual(windows, expected_dates)

    def test_get_year_windows_disparete_windows(self):
        expected_dates = [
            {'initDate': '1970-01-01', 'endDate': '2000-01-01'},
            {'initDate': '2000-01-02', 'endDate': '2000-01-02'},
        ]
        windows = get_year_windows('1970-01-01', '2000-01-02', 30)

        self.assertEqual(windows, expected_dates)

    def test_get_year_windows_start_and_end_the_same_day(self):
        expected_dates = [
            {'initDate': '1970-01-01', 'endDate': '1999-01-01'},
        ]
        windows = get_year_windows('1970-01-01', '1999-01-01', 30)

        self.assertEqual(windows, expected_dates)

    def test_get_year_windows_start_greater_than_end(self):
        expected_dates = []
        windows = get_year_windows('2000-01-01', '1999-01-01', 30)

        self.assertEqual(windows, expected_dates)

    def test_get_year_windows_leap_years(self):
        expected_date = [
            {'initDate': '2024-02-29', 'endDate': '2024-02-29'}
        ]

        windows = get_year_windows('2024-02-29', '2024-02-29', 30)

        self.assertEqual(windows, expected_date)
