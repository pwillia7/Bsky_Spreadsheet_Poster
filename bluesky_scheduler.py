"""
Bluesky Scheduler
=================

This script reads scheduled posts from a Google Sheet and publishes them to
Bluesky (AT Protocol) on or after their specified date/time.  It supports
multiple accounts via separate worksheets and uses a dedicated worksheet
(named ``Connections`` by default) to store each account's handle and app
password.  Posts may include plain text and a single image.  Once posted,
rows are marked as ``Posted`` along with the actual post URI and time of
publication.

Overview
--------

* ``Connections`` worksheet — Each row defines an account.  It should have
  columns ``Account`` (the human‑friendly tab name to use for scheduling),
  ``Handle`` (your ``user.bsky.social`` handle) and ``AppPassword`` (an
  app‑specific password generated in your Bluesky settings).
* One worksheet per account — The worksheet name should match the
  ``Account`` column value.  Each scheduling worksheet must have the
  following columns:

  - ``Datetime`` – When to publish the post.  Should be in ISO 8601 format
    (e.g. ``2025‑08‑05T10:30:00``).  A separate ``Timezone`` column can
    override the default timezone; otherwise the script falls back to
    ``America/Chicago``.
  - ``Content`` – The text of your post (up to 300 characters as per
    Bluesky’s limit).
  - ``Media`` – Optional.  A publicly accessible URL or local filename
    pointing to an image.  Supported formats are JPEG and PNG.  When
    provided, the script uploads the image to Bluesky as a blob and embeds
    it in the post.
  - ``Status`` – Internal state tracked by the script.  Blank or
    ``Scheduled`` rows are considered for posting.  After posting this
    column is updated to ``Posted`` or ``Error``.
  - ``PostURL`` – Populated after a successful post with the canonical
    ``https://bsky.app/profile/{did}/post/{cid}`` URL.
  - ``PostedAt`` – The actual timestamp (UTC) when the post was created.
  - ``ErrorMessage`` – Details on failures, if any.

Running the script
------------------

1. Create a Google Cloud service account with the Sheets API enabled.  Download
   its JSON credentials and share your scheduling sheet with the service
   account’s email address.  See the official
   ``gspread`` documentation for details.
2. Install dependencies in your Python environment:

   .. code-block:: bash

       pip install gspread google-auth requests python-dateutil pytz

   These packages are not installed in the coding‑assistant environment, but
   should be installed on the machine running the scheduler.
3. Fill in your ``Connections`` worksheet and scheduling worksheets.
4. Run the script (preferably periodically via ``cron`` or a task
   scheduler).  For example:

   .. code-block:: bash

       python bluesky_scheduler.py --sheet-id SPREADSHEET_ID --creds /path/to/creds.json

   Replace ``SPREADSHEET_ID`` with the ID from your sheet’s URL.

The script will process any row whose ``Datetime`` is in the past relative to
the current time in the specified timezone.  It logs actions to stdout and
updates the sheet in place.

Background
----------

Bluesky posts are created via the AT Protocol.  At a high level the process
consists of three steps:

1. Resolve your handle to a DID (decentralised identifier) using
   ``com.atproto.identity.resolveHandle``.
2. Create a session by exchanging your DID and app password for an access
   token via ``com.atproto.server.createSession``.
3. (Optional) Upload images as blobs using ``com.atproto.repo.uploadBlob`` and
   embed them in your post record.
4. Create the post record on your repository using
   ``com.atproto.repo.createRecord``.

The ``atproto`` Python library wraps these calls, but because it may not be
available in all environments, this script performs the HTTP requests
directly.  The blog post “How to post links on Bluesky with the atproto Python
library” demonstrates how to authenticate and post using the library’s
``Client`` class and ``send_post`` method【578713871347354†L20-L43】.  Another
article describing how to upload blobs notes that images must be uploaded
first and then referenced in the post record【208434675020982†L91-L110】.

"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import logging
import json
import os
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import re
import requests
import random
from dateutil import parser as dateparser  # type: ignore
import pytz  # type: ignore

try:
    import gspread  # type: ignore
    from google.oauth2.service_account import Credentials  # type: ignore
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    from googleapiclient.http import MediaIoBaseDownload
except ImportError:
    # gspread and google-auth are only required when the script is run in a
    # configured environment.  We avoid raising immediately so that the
    # remainder of this module remains importable for static analysis.
    gspread = None  # type: ignore
    Credentials = None  # type: ignore
    build = None # type: ignore
    HttpError = None # type: ignore
    MediaIoBaseDownload = None # type: ignore


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")


DEFAULT_TIMEZONE = "America/Chicago"
BLUESKY_BASE_URL = "https://bsky.social"


def resolve_handle(handle: str, *, base_url: str = BLUESKY_BASE_URL) -> str:
    """Resolve a Bluesky handle to its DID (Decentralised Identifier).

    Args:
        handle: The account handle, e.g. ``myuser.bsky.social``.
        base_url: Base URL of the Bluesky service.

    Returns:
        The DID string.
    """
    url = f"{base_url}/xrpc/com.atproto.identity.resolveHandle"
    params = {"handle": handle}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    did = data.get("did")
    if not did:
        raise RuntimeError(f"Could not resolve handle {handle}: {data}")
    return did


def create_session(did: str, app_password: str, *, base_url: str = BLUESKY_BASE_URL) -> str:
    """Create a session and obtain an access JWT.

    Args:
        did: The account’s DID.
        app_password: App password generated in Bluesky settings.
        base_url: Base URL of the Bluesky service.

    Returns:
        Access JWT (``accessJwt``) for authorised requests.
    """
    url = f"{base_url}/xrpc/com.atproto.server.createSession"
    payload = {"identifier": did, "password": app_password}
    headers = {"Content-Type": "application/json"}
    resp = requests.post(url, json=payload, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    access_jwt = data.get("accessJwt")
    if not access_jwt:
        raise RuntimeError(f"Failed to create session for {did}: {data}")
    return access_jwt


def upload_blob(image_content: bytes, access_jwt: str, *, mime_type: str = "image/jpeg", base_url: str = BLUESKY_BASE_URL) -> Dict:
    """Upload an image to Bluesky as a blob.

    Args:
        image_content: Binary content of the image.
        access_jwt: Access token from ``create_session``.
        mime_type: MIME type of the image (e.g. ``image/jpeg``).
        base_url: Base URL of the Bluesky service.

    Returns:
        A dictionary containing blob metadata (CID and MIME type) required for
        embedding in a post.
    """
    url = f"{base_url}/xrpc/com.atproto.repo.uploadBlob"
    headers = {
        "Content-Type": mime_type,
        "Authorization": f"Bearer {access_jwt}",
    }
    resp = requests.post(url, data=image_content, headers=headers, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    blob = data.get("blob")
    if not blob:
        raise RuntimeError(f"Failed to upload blob: {data}")
    return blob


def create_post_record(
    did: str,
    access_jwt: str,
    text: str,
    embed: Optional[Dict] = None,
    *,
    base_url: str = BLUESKY_BASE_URL,
) -> Dict:
    """Create a Bluesky post record.

    Args:
        did: The account DID.
        access_jwt: Access token for the session.
        text: Text content of the post (max 300 characters).
        embed: Optional embed dictionary for images.
        base_url: Base URL of the Bluesky service.

    Returns:
        Response JSON from the API containing the post URI and CID.
    """
    url = f"{base_url}/xrpc/com.atproto.repo.createRecord"
    now_utc = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
    record: Dict = {
        "repo": did,
        "collection": "app.bsky.feed.post",
        "record": {
            "text": text,
            "createdAt": now_utc.isoformat(timespec="seconds").replace("+00:00", "Z"),
        },
    }
    facets = extract_hashtag_facets(text)
    if facets:
        record["record"]["facets"] = facets
    if embed is not None:
        record["record"]["embed"] = embed

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_jwt}",
    }
    resp = requests.post(url, json=record, headers=headers, timeout=60)
    resp.raise_for_status()
    return resp.json()


def get_post_engagement(
    post_uri: str, access_jwt: str, *, base_url: str = BLUESKY_BASE_URL
) -> Dict:
    """Get engagement statistics for a specific post.

    Args:
        post_uri: The AT URI of the post.
        access_jwt: Access token for the session.
        base_url: Base URL of the Bluesky service.

    Returns:
        A dictionary with likeCount, repostCount, and replyCount.
    """
    url = f"{base_url}/xrpc/app.bsky.feed.getPostThread"
    params = {"uri": post_uri}
    headers = {"Authorization": f"Bearer {access_jwt}"}
    resp = requests.get(url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # The main post data is in thread.post
    post_data = data.get("thread", {}).get("post", {})
    return {
        "likeCount": post_data.get("likeCount", 0),
        "repostCount": post_data.get("repostCount", 0),
        "replyCount": post_data.get("replyCount", 0),
    }


def search_posts(query: str, access_jwt: str, *, base_url: str = BLUESKY_BASE_URL, limit: int = 100) -> List[str]:
    """Search for posts and return a list of author DIDs, with pagination.

    Args:
        query: The search query (e.g., a hashtag).
        access_jwt: Access token for the session.
        base_url: Base URL of the Bluesky service.
        limit: The number of results to return per page.

    Returns:
        A list of author DIDs from the search results.
    """
    url = f"{base_url}/xrpc/app.bsky.feed.searchPosts"
    dids = []
    cursor = None
    headers = {"Authorization": f"Bearer {access_jwt}"}

    # Fetch up to 3 pages of results to get a good number of candidates.
    for _ in range(3):
        params = {"q": query, "limit": limit}
        if cursor:
            params["cursor"] = cursor

        resp = requests.get(url, params=params, headers=headers, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        for post in data.get("posts", []):
            author_did = post.get("author", {}).get("did")
            if author_did:
                dids.append(author_did)

        cursor = data.get("cursor")
        if not cursor:
            break # No more pages

    return dids


def get_follows(actor_did: str, access_jwt: str, *, base_url: str = BLUESKY_BASE_URL) -> List[str]:
    """Get the list of DIDs that a user follows.
    Args:
        actor_did: The DID of the user.
        access_jwt: Access token for the session.
        base_url: Base URL of the Bluesky service.
    Returns:
        A list of DIDs of the users that the given user follows.
    """
    url = f"{base_url}/xrpc/app.bsky.graph.getFollows"
    params = {"actor": actor_did}
    headers = {"Authorization": f"Bearer {access_jwt}"}
    resp = requests.get(url, params=params, headers=headers, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    dids = []
    for follow in data.get("follows", []):
        did = follow.get("did")
        if did:
            dids.append(did)
    return dids


def follow_user(follower_did: str, did_to_follow: str, access_jwt: str, *, base_url: str = BLUESKY_BASE_URL) -> Dict:
    """Follow a user.
    Args:
        follower_did: The DID of the account that is doing the following.
        did_to_follow: The DID of the user to follow.
        access_jwt: Access token for the session.
        base_url: Base URL of the Bluesky service.
    Returns:
        The response from the createRecord call.
    """
    url = f"{base_url}/xrpc/com.atproto.repo.createRecord"
    now_utc = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
    record = {
        "repo": follower_did,
        "collection": "app.bsky.graph.follow",
        "record": {
            "$type": "app.bsky.graph.follow",
            "subject": did_to_follow,
            "createdAt": now_utc.isoformat(timespec="seconds").replace("+00:00", "Z"),
        },
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_jwt}",
    }
    resp = requests.post(url, json=record, headers=headers, timeout=60)
    resp.raise_for_status()
    return resp.json()


def get_account_stats(
    actor: str, access_jwt: str, *, base_url: str = BLUESKY_BASE_URL
) -> Dict:
    """Get statistics for a Bluesky account.

    Args:
        actor: The DID or handle of the account.
        access_jwt: Access token for the session.
        base_url: Base URL of the Bluesky service.

    Returns:
        A dictionary with followersCount, followsCount, and postsCount.
    """
    url = f"{base_url}/xrpc/app.bsky.actor.getProfile"
    params = {"actor": actor}
    headers = {"Authorization": f"Bearer {access_jwt}"}
    resp = requests.get(url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    return {
        "followersCount": data.get("followersCount", 0),
        "followsCount": data.get("followsCount", 0),
        "postsCount": data.get("postsCount", 0),
    }


@dataclass
class ConnectionInfo:
    """Holds authentication information for a single Bluesky account."""

    account_name: str
    handle: str
    app_password: str
    did: Optional[str] = None  # resolved lazily


class GoogleSheetClient:
    """Wrapper around gspread to simplify sheet operations."""

    def __init__(self, sheet_id: str, creds_path: str) -> None:
        if gspread is None or Credentials is None:
            raise ImportError("gspread and google-auth are required to use GoogleSheetClient."
                              "  Install them via pip and try again.")
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.readonly",
        ]
        creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
        gc = gspread.authorize(creds)
        self.sheet = gc.open_by_key(sheet_id)
        self.drive_service = build("drive", "v3", credentials=creds)

    def get_worksheet(self, name: str):
        try:
            return self.sheet.worksheet(name)
        except gspread.WorksheetNotFound:
            # If the worksheet doesn't exist, create it with header row
            ws = self.sheet.add_worksheet(title=name, rows=1000, cols=10)
            return ws

    def read_connections(self, worksheet_name: str = "Connections") -> List[ConnectionInfo]:
        ws = self.get_worksheet(worksheet_name)
        records = ws.get_all_records()
        connections: List[ConnectionInfo] = []
        for row in records:
            account = row.get("Account") or row.get("account")
            handle = row.get("Handle") or row.get("handle")
            app_pw = row.get("AppPassword") or row.get("app_password")
            if account and handle and app_pw:
                connections.append(ConnectionInfo(account, handle, app_pw))
        return connections

    def read_scheduled_posts(self, account_name: str) -> Tuple[List[str], List[List[str]]]:
        ws = self.get_worksheet(account_name)
        # Fetch header and all rows as lists; gspread returns cell values
        rows = ws.get_all_values()
        if not rows:
            return [], []
        headers = rows[0]
        return headers, rows[1:]

    def get_followed_users(self, worksheet_name: str = "FollowedUsers") -> set[str]:
        ws = self.get_worksheet(worksheet_name)
        all_values = ws.get_all_values()
        if not all_values:
            ws.append_row(["DID"])
            return set()

        # Assumes DIDs are in the first column, skipping the header
        return set(row[0] for row in all_values[1:])

    def add_followed_user(self, did: str, worksheet_name: str = "FollowedUsers") -> None:
        ws = self.get_worksheet(worksheet_name)
        ws.append_row([did])

    def update_row(self, account_name: str, row_index: int, headers: List[str], updates: Dict[str, str]) -> None:
        """Update specified columns in a row.  Row index is 1‑based and should
        refer to the sheet row (including header).  headers is the list of
        column names as returned from ``read_scheduled_posts``.  updates maps
        column names to new values.
        """
        ws = self.get_worksheet(account_name)
        # gspread uses 1‑based row/column indexing; row_index is offset from
        # header (0‑based).  So the actual row number in the sheet is row_index+2.
        sheet_row = row_index + 2
        # Build list of cells to update
        cells = []
        values = []
        for col_name, value in updates.items():
            try:
                col_num = headers.index(col_name) + 1
            except ValueError:
                continue
            cells.append((sheet_row, col_num, value))
        # Batch update
        cell_list = [ws.cell(r, c) for r, c, _ in cells]
        for cell, (_, _, val) in zip(cell_list, cells):
            cell.value = val
        ws.update_cells(cell_list)


def _get_gdrive_file_id(url: str) -> Optional[str]:
    """Extracts the Google Drive file ID from a URL."""
    match = re.search(r"/file/d/([a-zA-Z0-9_-]+)", url)
    if match:
        return match.group(1)
    match = re.search(r"id=([a-zA-Z0-9_-]+)", url)
    if match:
        return match.group(1)
    return None


def load_image(media_field: str, sheet_client: GoogleSheetClient) -> Tuple[bytes, str]:
    """Load image content and detect its MIME type.

    The ``media_field`` may be a URL (including Google Drive), or a local file
    path.  The function downloads the content if it looks like a URL;
    otherwise it reads from disk.  Supported file extensions are JPEG
    (.jpg/.jpeg) and PNG (.png).

    Returns a tuple of (binary content, mime type).
    """
    gdrive_file_id = _get_gdrive_file_id(media_field)

    if gdrive_file_id:
        try:
            file_metadata = sheet_client.drive_service.files().get(
                fileId=gdrive_file_id, fields="mimeType, name"
            ).execute()
            mime_type = file_metadata.get("mimeType", "image/jpeg")
            request = sheet_client.drive_service.files().get_media(fileId=gdrive_file_id)
            content = io.BytesIO()
            downloader = MediaIoBaseDownload(content, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            return content.getvalue(), mime_type
        except HttpError as error:
            raise RuntimeError(f"Could not download from Google Drive: {error}")

    if media_field.lower().startswith("http://") or media_field.lower().startswith("https://"):
        resp = requests.get(media_field, timeout=60)
        resp.raise_for_status()
        content = resp.content
        # Attempt to infer MIME type from Content‑Type header
        mime_type = resp.headers.get("Content-Type", "")
        if not mime_type:
            # Fallback to JPEG
            mime_type = "image/jpeg"
        return content, mime_type
    # Otherwise treat as local path
    path = os.path.expanduser(media_field)
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Media file not found: {path}")
    ext = os.path.splitext(path)[1].lower()
    if ext in (".jpg", ".jpeg"):
        mime_type = "image/jpeg"
    elif ext == ".png":
        mime_type = "image/png"
    else:
        raise ValueError(f"Unsupported image extension: {ext}")
    with open(path, "rb") as f:
        content = f.read()
    return content, mime_type


def parse_datetime(date_str: str, tz_name: str) -> dt.datetime:
    """Parse a date/time string into a timezone‑aware ``datetime``.

    ``dateutil.parser.parse`` can handle a variety of human‑friendly formats,
    including ISO 8601 (``2025-08-05T10:30:00``) and US‑style formats
    (``8/4/2025 14:40:00``).  To avoid misinterpreting day/month order on
    ambiguous numeric dates, ``dayfirst`` is explicitly set to ``False``,
    meaning the first number is treated as the month.  If your sheet uses
    day‑first formats (e.g. ``04/08/2025`` for 4 August 2025), set
    ``dayfirst=True`` here or use ISO 8601 dates instead.

    Args:
        date_str: The date/time string.
        tz_name: Timezone name (e.g. ``America/Chicago``).  If blank,
          ``DEFAULT_TIMEZONE`` is used.

    Returns:
        A timezone‑aware ``datetime`` object.
    """
    tz = pytz.timezone(tz_name or DEFAULT_TIMEZONE)
    # dayfirst=False ensures that ambiguous dates like "8/4/2025" are
    # interpreted as month/day/year.  Yearfirst defaults to False.
    dt_naive = dateparser.parse(date_str, dayfirst=False)
    if not dt_naive.tzinfo:
        dt_local = tz.localize(dt_naive)
    else:
        dt_local = dt_naive.astimezone(tz)
    return dt_local

def extract_hashtag_facets(text: str) -> List[Dict]:
    """Extract hashtag facets from post text with byte ranges."""
    facets = []
    for match in re.finditer(r"#(\w+)", text):
        tag = match.group(1)
        byte_start = len(text[:match.start()].encode("utf-8"))
        byte_end = byte_start + len(match.group(0).encode("utf-8"))
        facets.append({
            "index": {"byteStart": byte_start, "byteEnd": byte_end},
            "features": [{"$type": "app.bsky.richtext.facet#tag", "tag": tag}]
        })
    return facets

def process_account(sheet: GoogleSheetClient, conn: ConnectionInfo, now: dt.datetime, dry_run: bool = False) -> None:
    """Process scheduled posts for a single account.

    Args:
        sheet: The GoogleSheetClient instance.
        conn: ConnectionInfo for the account.
        now: Current time as an aware datetime in the default timezone.
        dry_run: If True, do not actually post; only log actions.
    """
    headers, rows = sheet.read_scheduled_posts(conn.account_name)
    if not headers:
        logger.debug("No rows found in %s", conn.account_name)
        return
    # Determine column indices
    try:
        dt_idx = headers.index("Datetime")
    except ValueError:
        logger.warning("Worksheet %s missing 'Datetime' column", conn.account_name)
        return
    content_idx = headers.index("Content") if "Content" in headers else None
    # Optional columns
    hashtags_idx = headers.index("Hashtags") if "Hashtags" in headers else None
    media_idx = headers.index("Media") if "Media" in headers else None
    tz_idx = headers.index("Timezone") if "Timezone" in headers else None
    status_idx = headers.index("Status") if "Status" in headers else None
    posturl_idx = headers.index("PostURL") if "PostURL" in headers else None
    postedat_idx = headers.index("PostedAt") if "PostedAt" in headers else None
    errmsg_idx = headers.index("ErrorMessage") if "ErrorMessage" in headers else None

    for row_i, row in enumerate(rows):
        # Skip if status indicates already posted
        status = row[status_idx] if status_idx is not None and len(row) > status_idx else ""
        if status and status.lower() not in ("", "scheduled"):
            continue

        dt_str = row[dt_idx] if len(row) > dt_idx else ""
        if not dt_str:
            continue
        tz_name = row[tz_idx] if tz_idx is not None and len(row) > tz_idx else DEFAULT_TIMEZONE
        scheduled_dt = parse_datetime(dt_str, tz_name)
        # Only process rows whose scheduled time has passed (or is equal to now)
        if scheduled_dt > now:
            continue

        # Build the text by combining content and hashtags (if provided).
        raw_text = row[content_idx] if content_idx is not None and len(row) > content_idx else ""
        hashtags = row[hashtags_idx] if hashtags_idx is not None and len(row) > hashtags_idx else ""
        text_parts: List[str] = []
        if raw_text:
            text_parts.append(str(raw_text).strip())
        if hashtags:
            text_parts.append(str(hashtags).strip())
        text = "\n".join(part for part in text_parts if part)
        media = row[media_idx] if media_idx is not None and len(row) > media_idx else ""

        logger.info("Posting row %d for account %s: %s", row_i + 2, conn.account_name, text)
        # Ensure we have DID and session
        embed = None
        # Sanitize text and enforce character limit
        text = text.encode("utf-8", errors="replace").decode("utf-8")

        if len(text) > 300:
            logger.warning("Post text is over 300 characters (%d). It may be rejected.", len(text))

        # Debug output for post
        logger.debug("Final post text:\n%s", text)
        logger.debug("Text length: %d", len(text))
        
        if embed:
            logger.debug("Embed payload:\n%s", json.dumps(embed, indent=2))

        try:
            if conn.did is None:
                conn.did = resolve_handle(conn.handle)
            access_jwt = create_session(conn.did, conn.app_password)
            embed = None
            if media:
                # Load and upload image
                img_bytes, mime_type = load_image(media, sheet)
                blob = upload_blob(img_bytes, access_jwt, mime_type=mime_type)
                embed = {
                    "$type": "app.bsky.embed.images",
                    "images": [
                        {
                            "image": blob,
                            "alt": "image",
                        }
                    ],
                }
            if dry_run:
                # Simulate success
                post_uri = "dry_run"
                cid = "dry_run"
            else:
                try:
                    result = create_post_record(conn.did, access_jwt, text, embed=embed)
                except requests.HTTPError as e:
                    logger.error("HTTPError while posting: %s", e)
                    logger.error("Response body: %s", e.response.text)
                    raise
                post_uri = result.get("uri")
                cid = result.get("cid")
            post_url = None
            if post_uri and cid:
                # Construct public URL: https://bsky.app/profile/{did}/post/{cid}
                # Use the DID portion of the URI (post_uri is like at://did/app.bsky.feed.post/cid)
                # The DID appears after at:// and before /app.bsky.feed.post
                # e.g. at://did:plc:abcdef/app.bsky.feed.post/xyz -> did:plc:abcdef
                # We'll extract the user_id from the DID as done in the example
                # "resolve_handle" ensures we already have the DID, but the URL uses the handle's DID
                user_did = conn.did
                # For now, fallback to handle because Bluesky's web domain uses the handle
                # Example: https://bsky.app/profile/<handle>/post/<post_id>
                # The post ID is after the last slash in URI
                post_id = post_uri.split("/")[-1] if isinstance(post_uri, str) else cid
                post_url = f"https://bsky.app/profile/{conn.handle}/post/{post_id}"
            updates = {
                "Status": "Posted",
                "PostURL": post_url or "",
                "PostedAt": dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat(timespec="seconds"),
                "ErrorMessage": "",
            }
            sheet.update_row(conn.account_name, row_i, headers, updates)
            logger.info("Posted successfully: %s", post_url)
        except Exception as exc:
            logger.error("Error posting row %d: %s", row_i + 2, exc)
            updates = {
                "Status": "Error",
                "ErrorMessage": str(exc),
            }
            sheet.update_row(conn.account_name, row_i, headers, updates)


def _reconstruct_at_uri(post_url: str, did: str) -> Optional[str]:
    """Reconstructs an AT URI from a public post URL and a DID."""
    if not post_url or not did:
        return None
    match = re.search(r"/post/([a-zA-Z0-9_-]+)$", post_url)
    if not match:
        logger.warning("Could not extract post ID from URL: %s", post_url)
        return None
    post_id = match.group(1)
    return f"at://{did}/app.bsky.feed.post/{post_id}"


def update_statistics_sheet(sheet: GoogleSheetClient, connections: List[ConnectionInfo], dry_run: bool = False) -> None:
    """Fetches engagement statistics and updates the 'Statistics' sheet."""
    stats_ws = sheet.get_worksheet("Statistics")
    if not dry_run:
        stats_ws.clear()
        headers = [
            "Account", "Followers", "Following", "Total Posts", "Total Likes",
            "Total Reposts", "Total Replies", "Last Updated",
        ]
        stats_ws.append_row(headers, value_input_option='USER_ENTERED')

    for conn in connections:
        logger.info("Fetching statistics for account: %s", conn.account_name)
        try:
            if conn.did is None:
                conn.did = resolve_handle(conn.handle)
            access_jwt = create_session(conn.did, conn.app_password)

            # Fetch account stats
            account_stats = get_account_stats(conn.did, access_jwt)
            followers = account_stats.get("followersCount", 0)
            following = account_stats.get("followsCount", 0)
            total_posts = account_stats.get("postsCount", 0)

            # Fetch and aggregate post engagement
            total_likes, total_reposts, total_replies = 0, 0, 0
            post_headers, post_rows = sheet.read_scheduled_posts(conn.account_name)

            if post_headers:
                try:
                    posturl_idx = post_headers.index("PostURL")
                    status_idx = post_headers.index("Status")
                except ValueError:
                    logger.warning("Worksheet %s missing 'PostURL' or 'Status' column", conn.account_name)
                    continue

                for row in post_rows:
                    status = row[status_idx] if len(row) > status_idx else ""
                    post_url = row[posturl_idx] if len(row) > posturl_idx else ""
                    if status.lower() == "posted" and post_url:
                        at_uri = _reconstruct_at_uri(post_url, conn.did)
                        if at_uri:
                            try:
                                engagement = get_post_engagement(at_uri, access_jwt)
                                total_likes += engagement.get("likeCount", 0)
                                total_reposts += engagement.get("repostCount", 0)
                                total_replies += engagement.get("replyCount", 0)
                            except requests.HTTPError as e:
                                if e.response.status_code == 404:
                                    logger.warning("Post not found, skipping: %s", at_uri)
                                else:
                                    logger.error("HTTP error fetching engagement for %s: %s", at_uri, e)
                        else:
                            logger.warning("Could not reconstruct AT URI for post URL: %s", post_url)

            logger.info(
                f"Stats for {conn.account_name}: "
                f"Followers={followers}, Following={following}, Posts={total_posts}, "
                f"Likes={total_likes}, Reposts={total_reposts}, Replies={total_replies}"
            )
            if not dry_run:
                now_utc = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat(timespec="seconds")
                stats_row = [
                    conn.account_name, followers, following, total_posts,
                    total_likes, total_reposts, total_replies, now_utc,
                ]
                stats_ws.append_row(stats_row, value_input_option='USER_ENTERED')
                logger.info("Successfully updated statistics for %s", conn.account_name)

        except Exception as e:
            logger.error("Failed to update statistics for %s: %s", conn.account_name, e, exc_info=True)


def follow_new_users(sheet: GoogleSheetClient, connections: List[ConnectionInfo], limit: int, dry_run: bool = False) -> None:
    """Find and follow new users.

    Args:
        sheet: The GoogleSheetClient instance.
        connections: A list of ConnectionInfo objects for the accounts.
        limit: The maximum number of new users to follow per account.
        dry_run: If True, do not actually follow users; only log actions.
    """
    logger.info(f"Starting to follow new users (limit: {limit} per account, dry_run: {dry_run})")

    # Get the "FollowedUsers" worksheet and read the DIDs.
    followed_dids_from_sheet = sheet.get_followed_users()

    # Define search queries
    search_queries = ["#art", "#photography", "#ai"]
    candidate_dids = set()

    # Use the first connection to get an access token for searching
    if not connections:
        logger.error("No connections available to perform searches.")
        return

    first_conn = connections[0]
    if first_conn.did is None:
        first_conn.did = resolve_handle(first_conn.handle)
    access_jwt = create_session(first_conn.did, first_conn.app_password)

    for query in search_queries:
        logger.info(f"Searching for posts with query: {query}")
        try:
            dids = search_posts(query, access_jwt)
            candidate_dids.update(dids)
        except Exception as e:
            logger.error(f"Error searching for posts with query '{query}': {e}")
            continue

    # Filter out our own accounts and already followed users
    own_dids = set()
    for conn in connections:
        if conn.did is None:
            conn.did = resolve_handle(conn.handle)
        own_dids.add(conn.did)

    candidate_dids -= own_dids
    candidate_dids -= followed_dids_from_sheet

    logger.info(f"Found {len(candidate_dids)} new potential users to follow.")

    candidate_list = list(candidate_dids)
    random.shuffle(candidate_list)

    all_followed_dids = set(followed_dids_from_sheet)

    for conn in connections:
        if conn.did is None:
            # This should have been resolved already, but just in case
            conn.did = resolve_handle(conn.handle)

        access_jwt = create_session(conn.did, conn.app_password)

        try:
            my_follows = set(get_follows(conn.did, access_jwt))

            followed_count = 0

            # Iterate over a copy of the list so we can modify it
            for candidate in list(candidate_list):
                if followed_count >= limit:
                    break

                if candidate in my_follows or candidate in all_followed_dids:
                    continue

                logger.info(f"Account '{conn.handle}' is attempting to follow {candidate}")
                if not dry_run:
                    try:
                        follow_user(conn.did, candidate, access_jwt)
                        logger.info(f"Successfully followed {candidate}")
                        sheet.add_followed_user(candidate)
                        all_followed_dids.add(candidate)
                        candidate_list.remove(candidate)
                        followed_count += 1
                    except Exception as e:
                        logger.error(f"Error following user {candidate}: {e}")
                else:
                    logger.info(f"[DRY RUN] Would follow {candidate}")
                    all_followed_dids.add(candidate)
                    candidate_list.remove(candidate)
                    followed_count += 1
        except Exception as e:
            logger.error(f"An error occurred while processing account {conn.handle}: {e}")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Schedule and post Bluesky updates from a Google Sheet.")
    parser.add_argument("--sheet-id", required=True, help="The ID of the Google Sheet.")
    parser.add_argument("--creds", required=True, help="Path to service account JSON credentials.")
    parser.add_argument("--timezone", default=DEFAULT_TIMEZONE, help="Default timezone for scheduled posts.")
    parser.add_argument("--run-once", action="store_true", help="Process due posts once and exit (default).")
    parser.add_argument("--interval", type=int, default=300, help="Polling interval in seconds when not using --run-once.")
    parser.add_argument("--dry-run", action="store_true", help="Do not post; just log actions.")
    parser.add_argument("--update-stats", action="store_true", help="Update the statistics sheet and exit.")
    parser.add_argument(
        "--follow-new-users",
        type=int,
        metavar="N",
        help="Follow up to N new users per account based on hashtags.",
    )
    args = parser.parse_args(argv)

    sheet = GoogleSheetClient(args.sheet_id, args.creds)
    connections = sheet.read_connections()
    if not connections:
        logger.error("No connections found in sheet.  Populate the 'Connections' worksheet.")
        return 1

    if args.update_stats:
        update_statistics_sheet(sheet, connections, dry_run=args.dry_run)
        return 0

    if args.follow_new_users:
        follow_new_users(sheet, connections, args.follow_new_users, dry_run=args.dry_run)
        return 0

    tz = pytz.timezone(args.timezone)
    if args.run_once:
        now = dt.datetime.now(tz)
        for conn in connections:
            process_account(sheet, conn, now, dry_run=args.dry_run)
    else:
        while True:
            now = dt.datetime.now(tz)
            for conn in connections:
                process_account(sheet, conn, now, dry_run=args.dry_run)
            time_to_sleep = max(args.interval, 30)
            logger.debug("Sleeping for %s seconds", time_to_sleep)
            try:
                import time
                time.sleep(time_to_sleep)
            except KeyboardInterrupt:
                logger.info("Scheduler interrupted.  Exiting.")
                break
    return 0


if __name__ == "__main__":
    sys.exit(main())