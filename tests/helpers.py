"""Test data builders for Bitcoin Miner Data Platform tests."""


def make_report(**overrides):
    defaults = {
        'ticker': 'MARA',
        'report_date': '2024-09-01',
        'published_date': '2024-09-03',
        'source_type': 'archive_pdf',
        'source_url': None,
        'raw_text': 'MARA mined 700 BTC in September 2024.',
        'parsed_at': '2024-09-03T12:00:00',
    }
    defaults.update(overrides)
    return defaults


def make_data_point(**overrides):
    defaults = {
        'report_id': None,
        'ticker': 'MARA',
        'period': '2024-09-01',
        'metric': 'production_btc',
        'value': 700.0,
        'unit': 'BTC',
        'confidence': 0.92,
        'extraction_method': 'prod_btc_0',
        'source_snippet': 'mined 700 BTC in September',
    }
    defaults.update(overrides)
    return defaults


def make_review_item(**overrides):
    defaults = {
        'data_point_id': None,
        'ticker': 'MARA',
        'period': '2024-09-01',
        'metric': 'production_btc',
        'raw_value': '700.0',
        'confidence': 0.65,
        'source_snippet': 'mined 700 BTC',
        'status': 'PENDING',
    }
    defaults.update(overrides)
    return defaults
