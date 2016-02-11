# service-worker

#### populate_redis_worker.py

Usage: 
```Shell
populate_redis_worker.py <size_of_leaderboards>
```

To increase size of leaderboard stored, increase <size_of_leaderboards>  (defaults to 10)

Requires running postgres with: dbname='halcyon' user='postgres' host='localhost' password='hi'
Requires running redis server with: host='localhost', port=6379, db=0

Change lang_list to include more languages to include.

Each zset in Redis is stored with a name of <lang>:<curr_week|prev_week>_<trend_modifier>
For example "JavaScript:curr_week_low".




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

TODO: Change current week query to include the the difference in metrics between it and the last week

This assumes will be running cron job at ~1am for previous days new stats.

