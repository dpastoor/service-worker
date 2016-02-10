# service-worker

#### populate_redis_worker.py

Requires running postgres with: dbname='halcyon' user='postgres' host='localhost' password='hi'
Requires running redis server with: host='localhost', port=6379, db=0

Change lang_list to include more languages to include.

To increase size of leaderboard stored, increase the 'limit 10' at the bottom of each query.

TODO: Change date range in queries to be:
```SQL
  date >= current_date - 7
  AND date <  current_date 
```
for current week, and 
```SQL
  date >= current_date - 14
  AND date <  current_date - 7
```
for previous week.

This assumes will be running cron job at ~1am for previous days new stats.