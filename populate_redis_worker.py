import psycopg2
import redis
import sys
import json
import os
lang_list = ['All','Python', 'JavaScript', 'R', 'Go']

# print("ENVIRONEMTAL VARS")
# print(os.environ)
try:
    if os.environ['POSTGRES_PORT_5432_TCP_PORT'] == '5432':
      print ("dbname='test-db' user='postgres' host='%s' password='password' port='5432' "%os.environ['POSTGRES_PORT_5432_TCP_ADDR'])
      try:
        conn = psycopg2.connect("dbname='test-db' user='postgres' host='%s' password='password'"%os.environ['POSTGRES_PORT_5432_TCP_ADDR'])
      except psycopg2.Error as e:
        print(e)
    else:
      conn = psycopg2.connect("dbname='halcyon' user='postgres' host='localhost' password='hi'")
      print ("Database connected in locally")
except:
    print ("I am unable to connect to the database")

try:
    if os.environ['REDIS_PORT_6379_TCP_PORT'] == '6379':
      r = redis.StrictRedis(host=os.environ['REDIS_PORT_6379_TCP_ADDR'], port=6379, db=0)
      print ("Redis connected in docker")
    else:
      r = redis.StrictRedis(host='localhost', port=6379, db=0)
      print ("Redis connected locally")
except:
    print ("I am unable to connect to redis")

def insertVelocities(num_repos, time_window):
  # set time windows
  if time_window == 'month':
    curr_date_low = "'2015-12-31'::date - 31"
    curr_date_high = "'2015-12-31'::date"
    prev_date_low = "'2015-12-31'::date - 31 - 31"
    prev_date_high = "'2015-12-31'::date - 31"
  elif time_window == 'week':
    curr_date_low = "'2015-12-31'::date - 7"
    curr_date_high = "'2015-12-31'::date"
    prev_date_low = "'2015-12-31'::date - 7 - 7"
    prev_date_high = "'2015-12-31'::date - 7"
  else: # 'day'
    curr_date_low = "'2015-12-31'::date - 1"
    curr_date_high = "'2015-12-31'::date"
    prev_date_low = "'2015-12-31'::date - 1 - 1"
    prev_date_high = "'2015-12-31'::date - 1"

  for lang in lang_list:
      for base_stars, exponent, trend_modifier in [[100, 6, 'low'], [400, 6, 'med'], [400, 8, 'high']]:
          print('currently processing %sly for: '%time_window, lang, base_stars, exponent)
          pipe = r.pipeline()
          # current week's
          pipe.delete('%s:curr_%s_%s'%(lang, time_window, trend_modifier))
          cur = conn.cursor()
          cur.execute(
              """select repo_name, description, language, num_stars, stars, stars::real / ((num_stars::real+%s)^(1.%s) ) AS normalized_stars
              From
              (
                  select repo_id, repo_name, description, language, num_stars, count(*) AS stars from halcyon."Test_Hourly_Watches"
                  Inner Join halcyon."Test_Repos" On repo_id = id
                  where num_stars > 0
                  AND date > %s
                  AND date <=  %s
                  group by repo_id, repo_name, description, language, num_stars order by num_stars desc
              ) AS x
              where language Like '%s'
              order by normalized_stars desc
              limit %s""" %(base_stars, exponent, curr_date_low, curr_date_high, (lang if (lang != 'All') else '%'), num_repos) )
          rows_curr = cur.fetchall()
          for row in rows_curr:
              repo_info = {"name":row[0], "description":row[1], "stars_in_%s"%time_window :row[4]}
              if lang == 'All':
                repo_info['language'] = row[2]
              repo_json = json.dumps(repo_info)
              repo_score = row[5]
              pipe.zadd('%s:curr_%s_%s'%(lang, time_window, trend_modifier), repo_score, repo_json)

          # prev week's
          pipe.delete('%s:prev_%s_%s'%(lang, time_window, trend_modifier)) 
          cur = conn.cursor()
          cur.execute(
              """select repo_name, description, language, num_stars, stars, stars::real / ((num_stars::real+%s)^(1.%s) ) AS normalized_stars
              From
              (
                  select repo_id, repo_name, description, language, num_stars, count(*) AS stars from halcyon."Test_Hourly_Watches"
                  Inner Join halcyon."Test_Repos" On repo_id = id
                  where num_stars > 0
                  AND date > %s
                  AND date <=  %s
                  group by repo_id, repo_name, description, language, num_stars order by num_stars desc
              ) AS x
              where language Like '%s'
              order by normalized_stars desc
              limit %s""" %(base_stars, exponent, prev_date_low, prev_date_high, (lang if (lang != 'All') else '%'), num_repos) )
          rows_prev = cur.fetchall()
          for row in rows_curr:
              repo_info = {"name":row[0], "description":row[1], "stars_in_%s"%time_window :row[4]}
              if lang == 'All':
                repo_info['language'] = row[2]
              repo_json = json.dumps(repo_info)
              repo_score = row[5]
              pipe.zadd('%s:curr_%s_%s'%(lang, time_window, trend_modifier), repo_score, repo_json)

          # insert and delete all as single transaction
          pipe.execute()



if __name__ == "__main__":
    try:
        num_of_repos_want_for_each_leaderboard = sys.argv[1]
        time_window = sys.argv[2]
        if time_window not in ['month', 'week', 'day']:
          raise ValueError("argv[2] should be a string. Usage: populate_redis_worker.py <num_of_repos> <'month'|'week'|'day'>\n");

    except IndexError:
        print("Usage: populate_redis_worker.py <num_of_repos> <'month'|'week'|'day'>\n ")
        sys.exit(1)

    # start inserting rows into redis
    insertVelocities(num_of_repos_want_for_each_leaderboard, time_window)
