update_headers.py - keeps bitcoin_block_headers.sqlite filled as new blocks are mined
==================================================================================

What it does
  Each run finds the highest mined height in your database, downloads the newer
  block headers from public Electrum servers (mempool.space / blockstream.info as
  backup), checks every one (prev-hash link, double-SHA256 hash, proof of work,
  difficulty retarget, median-time-past), confirms the new tip hash with at least
  2 other sources, then fills those rows (mined = 1) and recomputes the constraint
  representation of the rows still unmined. All in one transaction: if anything
  fails, nothing is written. If a block at the top was replaced by the network
  (a reorg), it rolls back to the fork point and refills.

Needs
  Python 3.8 or newer (python.org). Nothing else to install.
  Put update_headers.py in the same folder as bitcoin_block_headers.sqlite.
  About 1 GB of free disk space while it writes (SQLite's safety journal).
  An update with new blocks takes ~10-60 s (it refreshes 5.9M future rows).
  Close DB Browser / other programs that are writing to the file while it runs
  (reading is fine).

Windows
  Open Command Prompt in the folder (type cmd in the Explorer address bar), then:
      py update_headers.py            one update
      py update_headers.py --watch    keep updating every 10 min (Ctrl+C stops)
  Schedule it (every 10 min, no window needed): open Task Scheduler >
  Create Basic Task > Daily > Action "Start a program":
      Program:   py
      Arguments: "C:\path\to\update_headers.py" --db "C:\path\to\bitcoin_block_headers.sqlite"
  Then open the task's Properties > Triggers > Edit > "Repeat task every 10 minutes"
  for a duration of "Indefinitely".

Mac
  Open Terminal, cd to the folder, then:
      python3 update_headers.py           one update
      python3 update_headers.py --watch   keep updating every 10 min (Ctrl+C stops)
  Schedule it with cron: run  crontab -e  and add one line (use your own paths):
      */10 * * * * /usr/bin/python3 /Users/you/btc/update_headers.py --db /Users/you/btc/bitcoin_block_headers.sqlite >> /Users/you/btc/update.log 2>&1
  (If macOS asks, give Terminal/cron "Full Disk Access" in System Settings >
  Privacy & Security, or keep the files outside Desktop/Documents.)

Linux: same as Mac (python3, cron).

Options
  --db PATH               database file (default: current folder, then script folder)
  --watch                 loop forever
  --interval MIN          minutes between runs with --watch (default 10)
  --verbose               show each server's answer
  --refresh-constraints   only recompute the unmined-row constraint columns

Log (one line per run), e.g.
  2026-11-03 09:41:02  +5 blocks 974338-974342 | tip 974342 0000...85e8b | verified link/PoW/bits/MTP | tip confirmed by 15 other sources | 9.6s
  2026-11-03 09:51:03  no new blocks - tip 974342 ... (network 974342) | 1.6s
  ... ERROR: <reason> - nothing written     (it simply tries again next run)

The last change is also recorded inside the database:
  SELECT * FROM metadata WHERE key LIKE 'updater%' OR key LIKE 'tip%';

Note: most public Electrum servers use self-signed SSL certificates, so the script
does not rely on certificates for trust. Every header is verified by proof of work
and the tip must match several independent servers before anything is written.
