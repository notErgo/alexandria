"""Unit tests for scraper_mode_issue() validation helper (db.py)."""
from infra.db import scraper_mode_issue


class TestScraperModeIssueDrupalYear:
    def test_valid_row_returns_none(self):
        row = {
            'scraper_mode': 'drupal_year',
            'ir_url': 'https://bitfarms.com/investors/press-releases',
            'pr_start_date': '2023-01-01',
        }
        assert scraper_mode_issue(row) is None

    def test_missing_ir_url_returns_error(self):
        row = {
            'scraper_mode': 'drupal_year',
            'ir_url': '',
            'pr_start_date': '2023-01-01',
        }
        result = scraper_mode_issue(row)
        assert result == 'drupal_year mode missing ir_url'

    def test_none_ir_url_returns_error(self):
        row = {
            'scraper_mode': 'drupal_year',
            'ir_url': None,
            'pr_start_date': '2023-01-01',
        }
        result = scraper_mode_issue(row)
        assert result == 'drupal_year mode missing ir_url'

    def test_missing_pr_start_date_returns_error(self):
        row = {
            'scraper_mode': 'drupal_year',
            'ir_url': 'https://bitfarms.com/investors/press-releases',
            'pr_start_date': None,
        }
        result = scraper_mode_issue(row)
        assert result == 'drupal_year mode missing pr_start_date'

    def test_absent_pr_start_date_returns_error(self):
        row = {
            'scraper_mode': 'drupal_year',
            'ir_url': 'https://bitfarms.com/investors/press-releases',
        }
        result = scraper_mode_issue(row)
        assert result == 'drupal_year mode missing pr_start_date'
