import schedule
import time
from data.data import query_bsc

schedule.every(45).minutes.do(query_bsc)

while True:
    schedule.run_pending()
    time.sleep(1)
