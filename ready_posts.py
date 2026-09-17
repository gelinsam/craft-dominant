"""Finished, private social packages from reviewed owned assets.

Reuses Craft action packs, atomic snapshots, Flask delivery and zipfile. No
publishing API, scheduler, image editor, or new workflow framework is introduced.
"""
from datetime import date, datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import re
from urllib.parse import urlsplit, parse_qsl, urlencode, urlunsplit
import zipfile
from campaign_feedback import atomic_json, read_json


def root():
    return Path(os.environ.get('DB_PATH', 'craft_unified.db')).resolve().parent


def media_path(key):
    if not isinstance(key, str) or not re.fullmatch(r'[a-f0-9]{64}', key):
        raise ValueError('Invalid media identity')
    folder = root() / 'ready-post-media'
    path = folder / (key + '.jpg')
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 8_000_000:
        raise ValueError('Media unavailable')
    raw = path.read_bytes()
    if not raw.startswith(b'\xff\xd8\xff') or hashlib.sha256(raw).hexdigest() != key:
        raise ValueError('Media verification failed')
    return path


def ticket_url(value, post_id):
    parsed = urlsplit(value)
    if parsed.scheme != 'https' or parsed.netloc != 'www.eventbrite.com' or not re.fullmatch(r'/e/\d+', parsed.path):
        raise ValueError('Ticket destination is not verified')
    query = dict(parse_qsl(parsed.query))
    query.update(utm_source='instagram', utm_medium='organic_social', utm_campaign='craft_festival', utm_content=post_id)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query), ''))


def build_posts(packs, assets, now=None):
    now = now or datetime.now(timezone.utc)
    posts, held = [], []
    reviewed = []
    for asset in assets:
        try:
            if asset.get('reviewed') is not True or asset.get('cross_city_safe') is not True:
                continue
            media_path(asset['id'])
            reviewed.append(asset)
        except (OSError, ValueError, KeyError):
            continue
    for pack in packs:
        try:
            event_date = date.fromisoformat(pack.get('event_date') or '')
            if event_date < now.date():
                continue
            if not pack.get('ticket_links') or not pack.get('event_ids'):
                raise ValueError('Event destination pending')
            category = pack.get('event_type', 'coffee')
            pool = [a for a in reviewed if a.get('category') == category]
            if not pool:
                raise ValueError('Reviewed media pending')
            # Stable weekly choice; a source must be visually reviewed before reuse.
            pool.sort(key=lambda a: hashlib.sha256((pack['id']+a['id']).encode()).hexdigest())
            asset = pool[0]
            post_id = hashlib.sha256((pack['id']+'community').encode()).hexdigest()[:24]
            destinations = [ticket_url(u, post_id) for u in pack['ticket_links']]
            when = event_date.strftime('%B %-d, %Y')
            # Use the pack's full date range when present in its checked copy.
            caption = next((c['caption'] for c in pack.get('content', []) if c.get('format') == 'community'), None)
            if not caption and pack.get('content'):
                caption = pack['content'][0]['caption']
            if not caption:
                caption = f"Your coffee people. Your next great day out. ☕\n\n{pack['festival']} · {when}. Send this to the friend you want beside you, then choose your session together."
            caption += '\n\nTickets and sessions: link in bio.'
            posts.append({'id': post_id, 'festival': pack['festival'], 'city': pack['city'],
                          'event_date': pack['event_date'], 'event_ids': pack['event_ids'],
                          'week': pack['week'], 'caption': caption, 'media_id': asset['id'],
                          'alt': asset.get('alt', 'Festival community photo'),
                          'source_url': asset.get('source_url'), 'source_city': asset.get('source_city'),
                          'selection_basis': 'Reviewed reusable festival photography; community format from the launch reconstruction.',
                          'performance_basis': asset.get('performance_basis', 'Performance not measured'),
                          'ticket_links': destinations, 'status': 'awaiting_owner_review',
                          'review_note': 'Confirm the account bio points to the intended event/session before publishing. Borrowed photography illustrates the festival experience.',
                          'execution_allowed': False})
        except (KeyError, TypeError, ValueError):
            held.append({'festival': pack.get('festival', 'Unknown festival'),
                         'reason': 'Event details, ticket destination or reviewed media are incomplete.'})
    return {'generated_at': now.isoformat(), 'posts': posts, 'held': held, 'execution_allowed': False}


def refresh_ready_posts():
    from launch_action_packs import read_packs
    packs = read_packs()
    if packs.get('status') != 'current':
        return {'status': 'source_unavailable'}
    catalog = read_json(root()/'reusable-reviewed-assets.json') or {}
    value = build_posts(packs.get('packs', []), catalog.get('assets', []))
    atomic_json(root()/'ready-posts.json', value)
    return {'status': 'prepared', 'posts': len(value['posts']), 'held': len(value['held'])}


def read_posts(now=None):
    now = now or datetime.now(timezone.utc)
    value = read_json(root()/'ready-posts.json') or {'posts': [], 'held': [], 'execution_allowed': False}
    try:
        age = (now-datetime.fromisoformat(value['generated_at'])).total_seconds()
        value['status'] = 'current' if 0 <= age < 86400 else 'stale'
    except (ValueError, KeyError, TypeError):
        value['status'] = 'not_prepared'
    return value


def package(post_id):
    if not re.fullmatch(r'[a-f0-9]{24}', post_id):
        raise ValueError('Invalid post identity')
    value = read_posts()
    if value['status'] != 'current':
        raise ValueError('Prepared posts need refreshing')
    post = next((p for p in value['posts'] if p['id'] == post_id), None)
    if post is None or date.fromisoformat(post['event_date']) < date.today():
        raise ValueError('Post unavailable')
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, 'w', zipfile.ZIP_DEFLATED) as bundle:
        bundle.write(media_path(post['media_id']), 'photo.jpg')
        bundle.writestr('caption.txt', post['caption'])
        bundle.writestr('review-and-links.txt', post['review_note']+'\n\nTicket session links:\n'+'\n'.join(post['ticket_links'])+'\n\nSource: '+str(post['source_url']))
    stream.seek(0)
    return stream
