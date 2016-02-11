import psycopg2
import redis
import sys
import json

lang_list = ['All','Python', 'JavaScript', 'R', 'Go'] # empty string is all languages

try:
    conn = psycopg2.connect("dbname='halcyon' user='postgres' host='localhost' password='hi'")
except:
    print ("I am unable to connect to the database")

try:
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
except:
    print ("I am unable to connect to redis")

def insertVelocities(num_repos=10):
  for lang in lang_list:
      for base_stars, exponent, trend_modifier in [[100, 6, 'low'], [400, 6, 'med'], [400, 8, 'high']]:
          print('currently processing weekly for: ', lang, base_stars, exponent)
          pipe = r.pipeline()
          # current week's
          pipe.delete('%s:curr_week_%s'%(lang, trend_modifier))
          cur = conn.cursor()
          cur.execute(
              """select repo_name, description, language, num_stars, stars, stars::real / ((num_stars::real+%s)^(1.%s) ) AS normalized_stars
              From
              (
                  select repo_id, repo_name, description, language, num_stars, count(*) AS stars from halcyon."Test_Hourly_Watches"
                  Inner Join halcyon."Test_Repos" On repo_id = id
                  where num_stars > 0
                  AND date > '2015-12-24'
                  AND date <=  '2015-12-31'
                  group by repo_id, repo_name, description, language, num_stars order by num_stars desc
              ) AS x
              where language Like '%s'
              order by normalized_stars desc
              limit %s""" %(base_stars, exponent, (lang if (lang != 'All') else '%'), num_repos) )
          rows_curr = cur.fetchall()
          for row in rows_curr:
              repo_info = {"name":row[0], "description":row[1], "stars_in_week":row[4]}
              if lang == 'All':
                repo_info['language'] = row[2]
              repo_json = json.dumps(repo_info)
              repo_score = row[5]
              pipe.zadd('%s:curr_week_%s'%(lang, trend_modifier), repo_score, repo_json)

          # prev week's
          pipe.delete('%s:prev_week_%s'%(lang, trend_modifier))
          cur = conn.cursor()
          cur.execute(
              """select repo_name, description, language, num_stars, stars, stars::real / ((num_stars::real+%s)^(1.%s) ) AS normalized_stars
              From
              (
                  select repo_id, repo_name, description, language, num_stars, count(*) AS stars from halcyon."Test_Hourly_Watches"
                  Inner Join halcyon."Test_Repos" On repo_id = id
                  where num_stars > 0
                  AND date > '2015-12-17'
                  AND date <=  '2015-12-24'
                  group by repo_id, repo_name, description, language, num_stars order by num_stars desc
              ) AS x
              where language Like '%s'
              order by normalized_stars desc
              limit %s""" %(base_stars, exponent, (lang if (lang != 'All') else '%'), num_repos) )
          rows_prev = cur.fetchall()
          for row in rows_curr:
              repo_info = {"name":row[0], "description":row[1], "stars_in_week":row[4]}
              if lang == 'All':
                repo_info['language'] = row[2]
              repo_json = json.dumps(repo_info)
              repo_score = row[5]
              pipe.zadd('%s:curr_week_%s'%(lang, trend_modifier), repo_score, repo_json)

          # insert and delete all as single transaction
          pipe.execute()



if __name__ == "__main__":
    try:
        num_of_repos_want_for_each_leaderboard = sys.argv[1]
    except IndexError:
        print("Usage: populate_redis_worker.py <num_of_repos>\n ")
        sys.exit(1)

    # start inserting rows into redis
    insertVelocities(num_of_repos_want_for_each_leaderboard)
