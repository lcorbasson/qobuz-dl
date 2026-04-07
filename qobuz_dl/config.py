import os

from qobuz_dl.color import *

HOWTO_RESET = "Reset your credentials with 'qobuz-dl -r'"
HOWTO_FRESH_TOKEN_FROM_BROWSER = (
    "Get a fresh token from your browser:\n"
    "  DevTools [F12] -> Network -> user/login POST -> Response -> user_auth_token"
)
HOWTO_FRESH_TOKEN_TO_CONFIG = HOWTO_RESET + "\n  (paste the token as the password)"
HOWTO_FRESH_TOKEN = HOWTO_FRESH_TOKEN_FROM_BROWSER + "\n" + HOWTO_FRESH_TOKEN_TO_CONFIG

if os.name == "nt":
    OS_CONFIG = os.environ.get("APPDATA")
else:
    OS_CONFIG = os.path.join(os.environ["HOME"], ".config")
CONFIG_PATH = os.path.join(OS_CONFIG, "qobuz-dl")
CONFIG_FILE = os.path.join(CONFIG_PATH, "config.ini")
QOBUZ_DB = os.path.join(CONFIG_PATH, "qobuz_dl.db")

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:83.0) Gecko/20100101 Firefox/83.0"
