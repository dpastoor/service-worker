import psycopg2
import redis

lang_list = ['Python', 'JavaScript', 'R', 'Go']

try:
    conn = psycopg2.connect("dbname='halcyon' user='postgres' host='localhost' password='hi'")
except:
    print ("I am unable to connect to the database")
    
try:    
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
except:
    print ("I am unable to connect to redis")


for lang in lang_list: 
    for base_stars, exponent in [[400, 6], [100, 6], [400, 8]]:
        print(base_stars, exponent)
        pipe = r.pipeline()
        # current week's
        pipe.delete('%s:curr_week_%s_%s'%(lang, base_stars, exponent))
        cur = conn.cursor()
        cur.execute(  
            """select repo_name, language, num_stars, stars, stars::real / ((num_stars::real+%s)^(1.%s) ) AS normalized_stars
            From 
            (
                select repo_id, repo_name, language, num_stars, count(*) AS stars from halcyon."Test_Hourly_Watches" 
                Inner Join halcyon."Test_Repos" On repo_id = id
                where num_stars > 0 
                AND date > '2015-12-24'
                AND date <=  '2015-12-31'
                group by repo_id, repo_name, language, num_stars order by num_stars desc
            ) AS x 
            where language Like '%s'
            order by normalized_stars desc""" %(base_stars, exponent, lang) )
        rows_curr = cur.fetchall()
        for row in rows_curr[:10]:
            pipe.zadd('%s:curr_week_%s_%s'%(lang, base_stars, exponent), row[4], row[0])
        
        # prev week's
        pipe.delete('%s:prev_week_%s_%s'%(lang, base_stars, exponent))
        cur = conn.cursor()
        cur.execute(  
            """select repo_name, language, num_stars, stars, stars::real / ((num_stars::real+%s)^(1.%s) ) AS normalized_stars
            From 
            (
                select repo_id, repo_name, language, num_stars, count(*) AS stars from halcyon."Test_Hourly_Watches" 
                Inner Join halcyon."Test_Repos" On repo_id = id
                where num_stars > 0 
                AND date > '2015-12-17'
                AND date <=  '2015-12-24'
                group by repo_id, repo_name, language, num_stars order by num_stars desc
            ) AS x 
            where language Like '%s'
            order by normalized_stars desc""" %(base_stars, exponent, lang) )
        rows_prev = cur.fetchall()
        for row in rows_prev[:10]:
            pipe.zadd('%s:prev_week_%s_%s'%(lang, base_stars, exponent), row[4], row[0])

        # insert and delete all as single transaction
        pipe.execute()

