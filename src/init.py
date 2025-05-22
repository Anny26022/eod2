import logging
import sys
import requests
import time
from argparse import ArgumentParser

from nse import NSE
from defs import defs
from defs.utils import writeJson

logger = logging.getLogger(__name__)

# Set the sys.excepthook to the custom exception handler
sys.excepthook = defs.log_unhandled_exception

# Original NSE class methods we need to preserve
original_init = NSE.__init__
original_req = NSE._NSE__req

def get_nse_data(self, url, params=None):
    """Modified request function that uses ScraperAPI"""
    logger.info(f"Fetching via ScraperAPI: {url}")
    
    # First get cookies from NSE
    cookies = {}
    try:
        cookie_resp = requests.get("https://www.nseindia.com/", headers={
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36"
        })
        cookies = cookie_resp.cookies.get_dict()
    except Exception as e:
        logger.warning(f"Failed to get NSE cookies: {e}")
    
    payload = {
        'api_key': '492fed55ee317f3d46a5336e5bda77b8',
        'url': url,
        'keep_headers': 'true',
        'device_type': 'desktop',
        'cookies': '; '.join([f"{k}={v}" for k, v in cookies.items()]) if cookies else ''
    }
    
    if params:
        # If the URL already has query parameters, append new ones
        if '?' in url:
            url += '&' + '&'.join(f'{k}={v}' for k, v in params.items())
        else:
            url += '?' + '&'.join(f'{k}={v}' for k, v in params.items())
        payload['url'] = url
        
    r = requests.get('https://api.scraperapi.com/', params=payload)
    logger.info(f"ScraperAPI Response Status: {r.status_code}")
    return r

def new_init(self, dir_path, server=False):
    """Modified init that handles cookies"""
    self.dir = dir_path
    self.server = server
    self.timeout = 10  # Default timeout in seconds
    self._NSE__session = requests.Session()
    self._NSE__session.headers.update({
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9'
    })

def exit(self):
    """Modified exit method that safely closes the session"""
    if hasattr(self, '_NSE__session'):
        self._NSE__session.close()

# Monkey patch the NSE class
NSE.__init__ = new_init
NSE._NSE__req = get_nse_data
NSE.exit = exit

parser = ArgumentParser(prog="init.py")

group = parser.add_mutually_exclusive_group()

group.add_argument(
    "-v", "--version", action="store_true", help="Print the current version."
)

group.add_argument(
    "-c", "--config", action="store_true", help="Print the current config."
)

args = parser.parse_args()

if args.version:
    sys.exit(f"EOD2 init.py: version {defs.config.VERSION}")

if args.config:
    sys.exit(str(defs.config))

# download the latest special_sessions.txt from eod2_data repo
special_sessions = defs.downloadSpecialSessions()

try:
    nse = NSE(defs.DIR)
except (TimeoutError, ConnectionError) as e:
    logger.warning(
        f"Network error connecting to NSE - Please try again later. - {e!r}"
    )
    sys.exit(1)

if defs.config.AMIBROKER and not defs.isAmiBrokerFolderUpdated():
    defs.updateAmiBrokerRecords(nse)

if "DLV_PENDING_DATES" not in defs.meta:
    defs.meta["DLV_PENDING_DATES"] = []

if len(defs.meta["DLV_PENDING_DATES"]):
    pendingList = defs.meta["DLV_PENDING_DATES"].copy()

    logger.info("Updating pending delivery reports.")

    for dateStr in pendingList:
        if defs.updatePendingDeliveryData(nse, dateStr):
            writeJson(defs.META_FILE, defs.meta)

while True:
    if not defs.dates.nextDate():
        nse.exit()
        sys.exit(0)  # Clean exit

    if defs.checkForHolidays(nse, special_sessions):
        defs.meta["lastUpdate"] = defs.dates.lastUpdate = defs.dates.dt
        writeJson(defs.META_FILE, defs.meta)
        continue

    # Validate NSE actions file
    defs.validateNseActionsFile(nse)

    # Download all files and validate for errors
    logger.info("Downloading Files")

    try:
        # NSE bhav copy
        BHAV_FILE = nse.equityBhavcopy(defs.dates.dt)

        # Index file
        INDEX_FILE = nse.indicesBhavcopy(defs.dates.dt)
    except (RuntimeError, Exception) as e:
        if defs.dates.dt.weekday() == 5:
            if defs.dates.dt != defs.dates.today:
                logger.info(
                    f'{defs.dates.dt:%a, %d %b %Y}: Market Closed\n{"-" * 52}'
                )

                # On Error, dont exit on Saturdays, if trying to sync past dates
                continue

            # If NSE is closed and report unavailable, inform user
            logger.info(
                "Market is closed on Saturdays. If open, check availability on NSE"
            )

        # On daily sync exit on error
        nse.exit()
        logger.warning(e)
        sys.exit(1)  # Error exit

    try:
        # NSE delivery
        DELIVERY_FILE = nse.deliveryBhavcopy(defs.dates.dt)
    except (RuntimeError, Exception):
        defs.meta["DLV_PENDING_DATES"].append(defs.dates.dt.isoformat())
        DELIVERY_FILE = None
        logger.warning(
            "Delivery Report Unavailable. Will retry in subsequent sync"
        )

    try:
        defs.updateNseEOD(BHAV_FILE, DELIVERY_FILE)

        # INDEX sync
        defs.updateIndexEOD(INDEX_FILE)
    except Exception as e:
        # rollback
        logger.exception("Error during data sync.", exc_info=e)
        defs.rollback(defs.DAILY_FOLDER)
        defs.cleanup((BHAV_FILE, DELIVERY_FILE, INDEX_FILE))

        defs.meta["lastUpdate"] = defs.dates.lastUpdate
        writeJson(defs.META_FILE, defs.meta)
        nse.exit()
        sys.exit(1)

    # No errors continue

    # Adjust Splits and bonus
    try:
        defs.adjustNseStocks()
    except Exception as e:
        logger.exception(
            "Error while making adjustments.\nAll adjustments have been discarded.",
            exc_info=e,
        )

        defs.rollback(defs.DAILY_FOLDER)
        defs.cleanup((BHAV_FILE, DELIVERY_FILE, INDEX_FILE))

        defs.meta["lastUpdate"] = defs.dates.lastUpdate
        writeJson(defs.META_FILE, defs.meta)
        nse.exit()
        sys.exit(1)

    if defs.hook and hasattr(defs.hook, "on_complete"):
        defs.hook.on_complete()

    defs.cleanup((BHAV_FILE, DELIVERY_FILE, INDEX_FILE))

    if defs.dates.today == defs.dates.dt:
        defs.cleanOutDated()

    defs.meta["lastUpdate"] = defs.dates.lastUpdate = defs.dates.dt
    writeJson(defs.META_FILE, defs.meta)

    logger.info(f'{defs.dates.dt:%d %b %Y}: Done\n{"-" * 52}')
    nse.exit()  # Call exit through the NSE instance

    if defs.dates.today == defs.dates.dt:
        defs.cleanOutDated()

    defs.meta["lastUpdate"] = defs.dates.lastUpdate = defs.dates.dt
    writeJson(defs.META_FILE, defs.meta)

    logger.info(f'{defs.dates.dt:%d %b %Y}: Done\n{"-" * 52}') 
