from argparse import ArgumentParser, FileType
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import sys

import xmltodict


def log(text: str) -> None:
    tstamp = datetime.now(tz=timezone.utc).isoformat()
    sys.stderr.write(f"[{tstamp}] {text}\n")


def safe_filename(fname: str) -> str:
    return "".join(c for c in fname if c.isalnum() or c in (" ", ".", "_", "-")).rstrip()    


def execute(con, *args, **kwargs):
    # log(f"Executing query {args[0]}")
    res = con.execute(*args, **kwargs)
    # log(f"Done executing query {args[0]}")
    return res


def get_database(database_path: str):
    should_create = not Path(database_path).exists()
    if should_create:
        log(f"Initializing database at {database_path}")
        with sqlite3.connect(database_path) as con:
            cur = con.cursor()
            execute(
                cur,
                """
                CREATE TABLE feed (
                    feed_id INTEGER PRIMARY KEY AUTOINCREMENT
                    , date_added TEXT
                    , feed_url TEXT
                    , title TEXT
                )
                """
            )
            execute(cur, "CREATE INDEX feed_feed_id ON feed(feed_id)")
            execute(
                cur,
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
            execute(cur, "CREATE INDEX raw_feed_raw_feed_id ON raw_feed(raw_feed_id)")
            execute(cur, "CREATE INDEX raw_feed_feed_id ON raw_feed(feed_id)")
            execute(
                cur,
                """
                CREATE TABLE feed_item (
                    feed_item_id INTEGER PRIMARY KEY AUTOINCREMENT
                    , feed_id INTEGER
                    , guid TEXT
                    , date_added TEXT
                    , title TEXT
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
            execute(cur, "CREATE INDEX feed_item_feed_item_id ON feed_item(feed_item_id)")
            execute(cur, "CREATE INDEX feed_item_feed_id ON feed_item(feed_id)")
            con.commit()
    con = sqlite3.connect(database_path)
    con.row_factory = sqlite3.Row
    return con


def list_feeds(con):
    cur = execute(con, "SELECT * FROM feed")
    for row in cur.fetchall():
        print(json.dumps(dict(row)))


def get_feed(con, feed_id):
    cur = execute(con, "SELECT * FROM feed WHERE feed_id = ?", (feed_id,))
    feed = cur.fetchone()
    if not feed:
        return
    feed = dict(feed)

    cur = execute(con, "SELECT * FROM feed_item WHERE feed_id = ? ORDER BY pub_date DESC LIMIT 10", (feed_id,))
    feed["items"] = [dict(i) for i in cur.fetchall()]

    cur = execute(con, "SELECT * FROM raw_feed WHERE feed_id = ? ORDER BY date_added DESC LIMIT 1", (feed_id,))
    feed["raw_feed"] = dict(cur.fetchone())
    feed["raw_feed"]["data"] = json.loads(feed["raw_feed"]["data"])
    
    print(json.dumps(feed))


def list_items(con, feed_id=None, title=None, description=None):
    cur = execute(
        con, 
        """
        SELECT * 
        FROM feed_item 
        WHERE (CASE WHEN ? IS NULL THEN TRUE ELSE feed_id = ? END)
            AND (CASE WHEN ? IS NULL THEN TRUE ELSE title LIKE ? END)
            AND (CASE WHEN ? IS NULL THEN TRUE ELSE description LIKE ? END)
        ORDER BY pub_date
        """, 
        (feed_id, feed_id, title, f"%{title}%", description, f"%{description}%")
    )
    for row in cur.fetchall():
        print(json.dumps(dict(row)))


def get_raw_feed(con, feed_id):
    cur = execute(con, "SELECT * FROM raw_feed WHERE feed_id = ? ORDER BY date_added DESC LIMIT 1", (feed_id,))
    feed = cur.fetchone()
    if not feed:
        return
    feed = dict(feed)
    feed["data"] = json.loads(feed["data"])
    print(json.dumps(feed))


def main():
    def feed_argument_parser(parent):
        parsers = parent.add_subparsers(required=True)
    
        list_p = parsers.add_parser("list")
        list_p.set_defaults(func=list_feeds)

        get_p = parsers.add_parser("get")
        get_p.add_argument("feed_id")
        get_p.set_defaults(func=get_feed)


    def feed_item_argument_parser(parent):
        parsers = parent.add_subparsers(required=True)

        list_p = parsers.add_parser("list")
        list_p.add_argument("--description")
        list_p.add_argument("--feed-id")
        list_p.add_argument("--title")
        list_p.set_defaults(func=list_items)

    
    def raw_argument_parser(parent):
        parsers = parent.add_subparsers(required=True)

        get_p = parsers.add_parser("get")
        get_p.add_argument("feed_id")
        get_p.set_defaults(func=get_raw_feed)


    parser = ArgumentParser()
    parser.add_argument("--database-path", "--db", "-d", default="feeds.db")

    table_command_parsers = parser.add_subparsers(required=True)
    feed_argument_parser(table_command_parsers.add_parser("feed"))
    feed_item_argument_parser(table_command_parsers.add_parser("item"))
    raw_argument_parser(table_command_parsers.add_parser("raw"))

    
    opts = vars(parser.parse_args())
    
    database_path = opts.pop("database_path")
    action_func = opts.pop("func")
    with get_database(database_path) as con:
        action_func(con, **opts)


if __name__ == "__main__":
    main()
