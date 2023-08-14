from argparse import ArgumentParser, FileType
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import sys
import time
import traceback

import requests
import xmltodict


def log(text: str, end: str = "\n") -> None:
    tstamp = datetime.now(tz=timezone.utc).isoformat()
    sys.stderr.write(f"[{tstamp}] {text}{end}")


def get_feed_xml(feed_url: str) -> str:
    if feed_url.startswith("file://"):
        with open(feed_url[7:]) as f:
            return f.read()
    else:
       res = requests.get(feed_url)
       res.raise_for_status()
       return res.text


def safe_filename(fname: str) -> str:
    return "".join(c for c in fname if c.isalnum() or c in (" ", ".", "_", "-")).rstrip()    


def checksum(data: str) -> str:
    hasher = hashlib.new("sha256")
    hasher.update(data.encode("utf8"))
    return hasher.hexdigest()


def get_database(database_path: str):
    should_create = not Path(database_path).exists()
    if should_create:
        log(f"Initializing database at {database_path}")
        with sqlite3.connect(database_path) as con:
            cur = con.cursor()
            cur.execute(
                """
                CREATE TABLE feed (
                    feed_id INTEGER PRIMARY KEY AUTOINCREMENT
                    , date_added TEXT
                    , feed_url TEXT
                    , title TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE raw_feed (
                    raw_feed_id INTEGER PRIMARY KEY AUTOINCREMENT
                    , feed_id INTEGER
                    , date_added TEXT
                    , data TEXT
                    , FOREIGN KEY (feed_id) REFERENCES feed(feed_id)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE feed_item (
                    feed_item_id INTEGER PRIMARY KEY AUTOINCREMENT
                    , feed_id INTEGER
                    , guid TEXT
                    , date_added TEXT
                    , title TEXT
                    , description TEXT
                    , pub_date TEXT
                    , itunes_duration TEXT
                    , enclosure_url TEXT
                    , enclosure_length INTEGER
                    , enclosure_type TEXT
                    , itunes_season INTEGER
                    , download_path TEXT
                    , FOREIGN KEY (feed_id) REFERENCES feed(feed_id)
                )
                """
            )
            con.commit()
    return sqlite3.connect(database_path)


def get_feed_xml(feed_url: str) -> str:
    if feed_url.startswith("file://"):
        with open(feed_url[7:]) as f:
            return f.read()
    else:
       res = requests.get(feed_url)
       res.raise_for_status()
       return res.text


def upsert_feed(con, feed_url: str, title: str, channel_dict: dict) -> int:
    cur = con.execute("SELECT feed_id FROM feed WHERE feed_url = ?", (feed_url,))
    feed_id_rec = cur.fetchone()
    if feed_id_rec is None:
        cur = con.execute(
            """
            INSERT INTO feed (date_added, feed_url, title)
            VALUES (CURRENT_TIMESTAMP, ?, ?)
            RETURNING feed_id
            """,
            (feed_url, title)
        )
        feed_id = cur.fetchone()[0]
    else:
        feed_id = feed_id_rec[0]
        con.execute(
            """
            UPDATE feed 
            SET title = ?
            WHERE feed_url = ?
            """,
            (title, feed_url)
        )

    con.execute(
        """
        INSERT INTO raw_feed (feed_id, date_added, data)
        VALUES (?, CURRENT_TIMESTAMP, ?)
        """,
        (feed_id, json.dumps(channel_dict))
    )

    return feed_id


def upsert_feed_item(
    con, 
    feed_id: int, 
    guid: str, 
    title: str, 
    description: str,
    pub_date: str, 
    itunes_duration: str,
    item_url: str, 
    item_length: int,
    item_type: str,
    itunes_season: int
) -> int:
    cur = con.execute("SELECT feed_item_id FROM feed_item WHERE feed_id = ? AND guid = ?", (feed_id, guid))
    feed_item_id_rec = cur.fetchone()
    if not feed_item_id_rec:
        cur = con.execute(
            """
            INSERT INTO feed_item (
                feed_id
                , guid
                , date_added
                , title
                , description
                , pub_date
                , itunes_duration
                , enclosure_url
                , enclosure_length
                , enclosure_type
                , itunes_season
            )
            VALUES (?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING feed_item_id
            """,
            (
                feed_id,
                guid,
                title,
                description,
                pub_date,
                itunes_duration,
                item_url,
                item_length,
                item_type,
                itunes_season,
            )
        )
        feed_item_id = cur.fetchone()[0]
    else:
        feed_item_id = feed_item_id_rec[0]
        con.execute(
            """
            UPDATE feed_item 
            SET 
                guid = ?
                , title = ?
                , description = ?
                , pub_date = ?
                , itunes_duration = ?
                , enclosure_url = ?
                , enclosure_length = ?
                , enclosure_type = ?
                , itunes_season = ?
            WHERE feed_item_id = ?
            """,
            (
                guid, 
                title, 
                description,
                pub_date, 
                itunes_duration, 
                item_url, 
                item_length, 
                item_type, 
                itunes_season,
                feed_item_id,
            )
        )

    return feed_item_id


def process_one_feed(con, feed_url):
    log(f"Downloading feed {feed_url}")
    feed_xml = get_feed_xml(feed_url)
    feed = xmltodict.parse(feed_xml)
    
    channel = feed["rss"]["channel"]
    title = channel["title"]

    feed_id = upsert_feed(con, feed_url, title, feed)

    feed_dir = Path(safe_filename(title))
    feed_dir.mkdir(parents=True, exist_ok=True)

    for item in channel["item"]:
        item_title = item["title"]
        item_filename = "".join(c for c in item_title if c.isalnum() or c in (" ", ".", "_", "-")).rstrip()

        item_url = item["enclosure"]["@url"]
        # Mon, 26 Dec 2022 05:00:00 GMT
        pub_date = datetime.strptime(item["pubDate"], "%a, %d %b %Y %H:%M:%S %Z")
        
        item_season = item.get("itunes:season")
        if item_season is not None:
            item_dir = Path(feed_dir, f"Season {item_season}")
            item_dir.mkdir(exist_ok=True)
        else:
            item_dir = feed_dir

        feed_item_id = upsert_feed_item(
            con,
            feed_id,
            item["guid"]["#text"],
            item_title,
            item["description"],
            pub_date.isoformat(),
            item.get("itunes:duration"),
            item_url,
            item["enclosure"]["@length"],
            item["enclosure"]["@type"],
            item_season,
        )

        cur = con.execute(
            "SELECT download_path FROM feed_item WHERE feed_item_id = ?", 
            (feed_item_id,)
        )
        item_file = cur.fetchone()[0]
        if item_file is None:
            item_date = pub_date.strftime("%Y-%m-%d")
            item_file = Path(item_dir, f"{item_date} {item_filename}.mp3")
        else:
            item_file = Path(item_file)

        if not item_file.exists():
            log(f"Downloading {item_file.as_posix()}")
            item_res = requests.get(item_url, stream=True)
            expected_bytes = int(item_res.headers["content-length"])
            bytes_downloaded = 0
            current_time = download_start_time = time.time()
            with item_file.open("wb") as f:
                for chunk in item_res.iter_content(chunk_size=2**16):
                    f.write(chunk)
                    bytes_downloaded += len(chunk)
                    current_time = time.time()
                    log(f"... downloaded {bytes_downloaded} of {expected_bytes} ({100 * bytes_downloaded / expected_bytes:.2f}%, {bytes_downloaded / 2**20 / (current_time - download_start_time):.2f}MBps)", end="\r")
                log(f"... downloaded {bytes_downloaded} of {expected_bytes} ({100 * bytes_downloaded / expected_bytes:.2f}%, {bytes_downloaded / 2**20 / (current_time - download_start_time):.2f}MBps)")

        
            con.execute(
                "UPDATE feed_item SET download_path = ? WHERE feed_item_id = ?", 
                (item_file.as_posix(), feed_item_id)
            )


def main():
    parser = ArgumentParser()
    parser.add_argument("--database-path", default="feeds.db")
    parser.add_argument("feeds_file", type=FileType("r"))
    opts = parser.parse_args()

    with get_database(opts.database_path) as con:
        for line in opts.feeds_file:
            line = line.strip()
            if line and not line.startswith("#"):
                try:
                    process_one_feed(con, line)
                    con.commit()
                except Exception:
                    log(f"ERROR processing {line}")
                    traceback.print_exc()
                    con.rollback()

if __name__ == "__main__":
    main()
