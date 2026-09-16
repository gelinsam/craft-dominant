"""Serve a private imported campaign report through the existing authenticated API.

Only the generated aggregate report is returned. The separate private source
bundle is retained for future re-analysis and is never exposed by this module.
"""
import base64
import json
import os
import zlib


def load_report():
    encoded = os.environ.get('CRAFT_CAMPAIGN_REPORT_ZLIB_B64', '')
    if not encoded or len(encoded) > 200_000:
        return None
    try:
        compressed = base64.b64decode(encoded, validate=True)
        decoder = zlib.decompressobj()
        raw = decoder.decompress(compressed, 2_000_001)
        if len(raw) > 2_000_000 or not decoder.eof or decoder.unused_data:
            return None
        report = json.loads(raw)
        expected = {'coffee', 'wine', 'cocktail', 'whiskey', 'beer'}
        if (report.get('version') != 1 or not report.get('as_of')
                or len(report.get('categories', [])) != 5
                or {r.get('category') for r in report['categories']} != expected):
            return None
        return report
    except (ValueError, TypeError, AttributeError, zlib.error):
        # Never log the private bundle or expose parser exceptions to callers.
        return None
