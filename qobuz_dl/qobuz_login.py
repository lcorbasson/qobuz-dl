#!/usr/bin/env python3
"""
Qobuz Token Updater
Paste your user_auth_token and this saves it straight to config.ini.

How to get your token from your browser:
  1. Log into qobuz.com
  2. Press F12 -> Network tab
  3. Refresh the page
  4. Click any request to qobuz.com/api.json
  5. In the Response JSON, find "user_auth_token"
  6. Copy that value and paste it here
"""

import configparser
import os
import sys

from qobuz_dl.config import (
    HOWTO_FRESH_TOKEN_FROM_BROWSER,
    CONFIG_FILE,
)


def load_config():
    if not os.path.exists(CONFIG_FILE):
        print(f"[!] Config not found at: {CONFIG_FILE}")
        print("    Run 'qobuz-dl' once first to generate it.")
        sys.exit(1)
    c = configparser.ConfigParser()
    c.read(CONFIG_FILE)
    return c


def save_token(token: str):
    c = load_config()
    c["DEFAULT"]["password"] = token
    with open(CONFIG_FILE, "w") as f:
        c.write(f)
    print(f"[✓] Token saved to: {CONFIG_FILE}")
    print("[✓] You can now run qobuz-dl normally.")


def main():
    print("=" * 45)
    print("       Qobuz Token Updater")
    print("=" * 45)
    print()
    print(HOWTO_FRESH_TOKEN_FROM_BROWSER)
    print()

    token = input("Paste token here: ").strip()

    if not token or len(token) < 20:
        print("[!] That doesn't look like a valid token.")
        sys.exit(1)

    save_token(token)


if __name__ == "__main__":
    main()
