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

# ScraperAPI configuration
SCRAPER_API_KEY = "492fed55ee317f3d46a5336e5bda77b8"
SCRAPER_API_BASE = "https://api.scraperapi.com/"

def get_nse_data(url, max_retries=3, initial_delay=5):
    logger.info(f"Fetching via ScraperAPI: {url}")
    
    for attempt in range(max_retries):
        try:
            payload = {
                'api_key': SCRAPER_API_KEY,
                'url': url,
                'keep_headers': 'true',
                'device_type': 'desktop'
            }
            r = requests.get(SCRAPER_API_BASE, params=payload)
            logger.info(f"ScraperAPI Response Status: {r.status_code}")
            
            # If successful or not a retriable error, return immediately
            if r.status_code in [200, 404]:
                return r
                
            # If we get rate limited or server error, retry
            if r.status_code in [429, 500, 502, 503, 504]:
                delay = initial_delay * (2 ** attempt)  # Exponential backoff
                logger.warning(f"Attempt {attempt + 1}/{max_retries} failed with status {r.status_code}. Retrying in {delay} seconds...")
                time.sleep(delay)
                continue
                
            # For other status codes, return the response
            return r
            
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:  # Last attempt
                logger.error(f"Failed to fetch data after {max_retries} attempts: {str(e)}")
                raise
            
            delay = initial_delay * (2 ** attempt)
            logger.warning(f"Attempt {attempt + 1}/{max_retries} failed with error: {str(e)}. Retrying in {delay} seconds...")
            time.sleep(delay)
    
    return r  # Return last response if we exit the loop

# Monkey patch the NSE class request method
NSE._NSE__req = get_nse_data

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
    exit(f"EOD2 init.py: version {defs.config.VERSION}")

if args.config:
    exit(str(defs.config))

# download the latest special_sessions.txt from eod2_data repo
special_sessions = defs.downloadSpecialSessions()

try:
    nse = NSE(defs.DIR)
except (TimeoutError, ConnectionError) as e:
    logger.warning(
        f"Network error connecting to NSE - Please try again later. - {e!r}"
    )
    exit()

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
        exit()

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
        exit()

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
        exit()

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
        exit()

    if defs.hook and hasattr(defs.hook, "on_complete"):
        defs.hook.on_complete()

    defs.cleanup((BHAV_FILE, DELIVERY_FILE, INDEX_FILE))

    if defs.dates.today == defs.dates.dt:
        defs.cleanOutDated()

    defs.meta["lastUpdate"] = defs.dates.lastUpdate = defs.dates.dt
    writeJson(defs.META_FILE, defs.meta)

    logger.info(f'{defs.dates.dt:%d %b %Y}: Done\n{"-" * 52}')
