# Bluesky Scheduler

This repository contains a simple Python application that lets you schedule posts to Bluesky using a Google Sheet as the “control panel.” Once configured, the scheduler will automatically check for due posts and publish them at the appropriate time, including posts with images. Subsequent updates to your schedule (adding new rows or changing existing ones) are automatically detected each time the scheduler runs.

## Overview

[An example Google Sheet can be found here.](https://docs.google.com/spreadsheets/d/1TjDAWDzI0UfU2CsvsULdd-ATAmGMSaD7aIs4RZVJG7g/edit?usp=sharing)

The scheduler reads a Google Sheet with the following structure:

* **Connections tab** – each row defines a Bluesky account to post from. Columns:
  - **Account** – a friendly name for the account; this becomes the name of a separate worksheet that holds posts for that account.
  - **Handle** – your Bluesky handle (e.g. `myuser.bsky.social`).
  - **AppPassword** – an app password generated in your Bluesky account settings.

* **One worksheet per account** – name it exactly as the `Account` value in the Connections tab. Columns:
  - **Datetime** – ISO 8601 date/time when the post should be published (e.g. `2025-08-05T10:30:00`). US‑style formats like `8/4/2025 14:40:00` also work; ambiguous dates are interpreted as month/day/year.
  - **Timezone** – (optional) IANA timezone (e.g. `America/Chicago`). Defaults to `America/Chicago` when blank.
  - **Content** – text of your post (up to 300 characters).
  - **Hashtags** - List of hashtags to be posted with the post content.
  - **Media** – (optional) publicly accessible URL, Google Drive URL, or local path to an image (JPEG or PNG). The script uploads the image as a blob before posting.
  - **Status** – leave blank; the scheduler updates it to `Posted` or `Error`.
  - **PostURL** – filled in after posting with the Bluesky URL.
  - **PostedAt** – the actual UTC time the post was created.
  - **ErrorMessage** – details if a post fails.

The scheduler uses the AT Protocol endpoints directly. For reference, the `atproto` Python SDK demonstrates how to authenticate and send a post with a link card:contentReference[oaicite:1]{index=1}, but this application performs the necessary HTTP calls without third‑party libraries.

## Prerequisites

1. **Python 3.8+** installed on the machine that will run the scheduler.
2. **Google Cloud service account** with the Sheets API enabled.
3. **Bluesky App Passwords** for each account you plan to post from.

### Creating a Google Service Account

1. Visit the [Google Cloud console](https://console.cloud.google.com/).
2. Create a new project or select an existing one.
3. Navigate to *APIs & Services → Dashboard* and click **Enable APIs and Services**. Search for and enable both the **Google Sheets API** and the **Google Drive API**.
4. Go to *APIs & Services → Credentials*, click **Create credentials → Service account**.
5. Give it a name (e.g. *Bluesky Scheduler SA*). You don’t need to assign any roles here.
6. After creating the service account, go to the **Keys** tab. Click **Add key → Create new key**, choose **JSON**, and download the key file. This file contains your credentials; keep it safe.
7. In your Google Sheet, share the document with the service account’s email address (found in the service account details or in the downloaded JSON under `client_email`). Grant it **Editor** access.

### Using Google Drive for Media

If you plan to use Google Drive to host your media, you will also need to share the folder containing your images and videos with the service account.

1. Create a folder in your Google Drive to store the media for your posts.
2. Share this folder with the service account's email address. Grant it **Viewer** access.
3. When you want to include media from this folder in a post, copy the shareable link for the file and paste it into the `Media` column in your spreadsheet.

## Setup

1. **Clone this repository** (or download the files).

2. **Install dependencies**. Create a virtual environment (optional) and install the required packages:

   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Prepare your Google Sheet** as described above. Copy the sheet’s ID from its URL (the long string between `/d/` and `/edit`).

4. **Generate Bluesky app passwords**. In each Bluesky account’s settings, navigate to *App Passwords* and create a new password. Save it somewhere secure – you’ll need it later.

5. **Fill out the `Connections` tab** in your sheet with the account name, handle and app password.

## Running the scheduler

Use the script `bluesky_scheduler.py` to process due posts. It accepts the following flags:

* `--sheet-id` – required. ID of your Google Sheet.
* `--creds` – required. Path to your service account JSON file.
* `--timezone` – optional. Default timezone (defaults to `America/Chicago`).
* `--run-once` – if provided, the script checks for due posts once and exits. Otherwise it will poll at a specified interval (default 300 seconds).
* `--interval` – polling interval in seconds when not using `--run-once`.
* `--dry-run` – don’t post anything; just log what would happen.

### One‑time run

To process all posts that are due right now and exit:

```bash
python bluesky_scheduler.py --sheet-id YOUR_SHEET_ID --creds /path/to/service_account.json --run-once
````
### Continuous scheduling

To keep the scheduler running indefinitely, omit `--run-once`. The script will wake up every `--interval` seconds (default 300) and process any rows whose scheduled time has passed:

````bash
python bluesky_scheduler.py --sheet-id YOUR_SHEET_ID --creds /path/to/service_account.json
````
You can add new rows or modify existing ones in your Google Sheet at any time. Each time the scheduler wakes up it reads fresh data from the sheet, so changes are automatically detected—no restart required.

### Running as a background service

For long‑running use, you might want to run the script as a background service. Here’s a basic example using `systemd` on Linux:

1. Create a unit file (e.g. `/etc/systemd/system/bluesky-scheduler.service`):

````ini
[Unit]
Description=Bluesky Scheduler
After=network.target

[Service]
Type=simple
WorkingDirectory=/path/to/repo
ExecStart=/usr/bin/env python bluesky_scheduler.py --sheet-id YOUR_SHEET_ID --creds /path/to/service_account.json
Restart=on-failure

[Install]
WantedBy=default.target
````

2. Reload systemd and enable the service:
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now bluesky-scheduler
```
This will start the scheduler at boot and restart it automatically if it crashes.

## Notes

* The scheduler uploads images as blobs before including them in posts, as recommended by Bluesky’s API:contentReference[oaicite:0]{index=0}.
* The script constructs the public post URL using your handle; if you migrate your account to a different host, update the handle in the `Connections` tab.
* Posts are created immediately once the scheduled time has passed. To avoid posting prematurely, double‑check your timezones and use ISO 8601 format.

## Troubleshooting

* **Authentication failures** – ensure the service account JSON path is correct and that the sheet is shared with the service account’s email.
* **Permission errors** – verify that your Bluesky app password is correct and that it has not been revoked.
* **Images not displaying** – confirm that the `Media` URL is publicly reachable or that the local path points to a valid JPEG/PNG file.
* **Timezones** – if posts are appearing too early or too late, specify a `Timezone` column per row or adjust the default timezone using the `--timezone` flag.

## Contributing

Feel free to open issues or pull requests for improvements.