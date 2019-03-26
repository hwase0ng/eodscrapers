'''
Usage: main [options] [COUNTER] ...

Arguments:
    COUNTER           Optional counters
Options:
    -d,--debug            Enable debug mode [default: False]
    -l,--list=<clist>     List of counters (dhkmwM) to retrieve from config.json
    -r,--resume           Resume file instead of generating from start [default: False]
    -s,--start=<sdt>      Start date
    -h,--help             This page

Created on Apr 16, 2018

@author: hwase0ng

Note: This version is adapted from a source found in Internet which I could no longer traced to
      provide its due credit. Please do inform me if you are the original author of this code.
'''

from common import appendCsv, getDataDir
from docopt import docopt
import pandas as pd
import settings as S
import time
from scrapers.investingcom.scrapeInvestingCom import loadIdMap, InvestingQuote
from utils.dateutils import getLastDate, getToday, getDaysBtwnDates,\
    getDayOffset, getNextDay
# sys.path.append('../../')


if __name__ == '__main__':
    args = docopt(__doc__)
    global DBG_ALL
    DBG_ALL = True if args['--debug'] else False
    # OUTPUT_FILE = sys.argv[1]
    idmap = loadIdMap("scrapers/hsi.idmap", dbg=DBG_ALL)

    WRITE_CSV = True
    S.RESUME_FILE = True if args['--resume'] else False

    stocks = ''

    if args['--start']:
        S.ABS_START = args['--start']
    if args['COUNTER']:
        stocks = args['COUNTER'][0].upper()

    rtn_code = 0
    OUTPUT_FILE = getDataDir(S.DATA_DIR, "eodscrapers") + 'investingcom/' + stocks + ".csv"
    TMP_FILE = OUTPUT_FILE + 'tmp'
    if S.RESUME_FILE:
        lastdt = getLastDate(OUTPUT_FILE)
        if len(lastdt) == 0:
            # File is likely to be empty, hence scrape from beginning
            lastdt = S.ABS_START
    else:
        lastdt = S.ABS_START
    enddt = getToday('%Y-%m-%d')
    print 'Scraping {0}: lastdt={1}, End={2}'.format(stocks, lastdt, enddt)
    failcount = 0
    while True:
        if failcount == 0:
            startdt = lastdt
            if getDaysBtwnDates(lastdt, enddt) > 22 * 3:  # do 3 months at a time
                stopdt = getDayOffset(startdt, 22 * 3)
                lastdt = getNextDay(stopdt)
            else:
                stopdt = enddt
        print "\tstart=%s, stop=%s" % (startdt, stopdt)
        eod = InvestingQuote(idmap, stocks, startdt, stopdt)
        if DBG_ALL:
            for item in eod:
                print item
        if len(eod.getCsvErr()) > 0:
            print eod.getCsvErr()
        elif isinstance(eod.response, unicode):
            dfEod = eod.to_df()
            if isinstance(dfEod, pd.DataFrame):
                failcount = 0
                if DBG_ALL:
                    print dfEod[:5]
                if WRITE_CSV:
                    dfEod.index.name = 'index'
                    dfEod.to_csv(TMP_FILE, index=False, header=False)
            else:
                failcount += 1
                time.sleep(3)
                print "ERR:" + dfEod + ": " + stocks + "," + lastdt
                rtn_code = -2
        else:
            failcount += 1
            time.sleep(3)
            print "ERR:" + eod.response + "," + lastdt
            rtn_code = -1

        if failcount == 0:
            time.sleep(2)
            appendCsv(rtn_code, OUTPUT_FILE)
        else:
            print "\tFailed: ", failcount
            if failcount > 5:
                break

        if stopdt == enddt:
            break
