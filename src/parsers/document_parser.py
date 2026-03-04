"""Dispatcher: returns correct parser for a given source_type."""


def get_parser(source_type: str):
    """Return the appropriate parser instance for a source_type."""
    if source_type in ('edgar_10k', 'edgar_10q'):
        from parsers.annual_report_parser import AnnualReportParser
        return AnnualReportParser()
    from parsers.press_release_parser import PressReleaseParser
    return PressReleaseParser()
