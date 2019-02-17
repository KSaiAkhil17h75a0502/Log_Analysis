#!/usr/bin/env python
import psycopg2


def main():
    db = psycopg2.connect("dbname=news")
    #  Trying to connect our database
    cursor = db.cursor()
    #  Query for the most popular three articles of all time
    popular = """
            select articles.title,count(*) from log,articles where
            log.path = '/article/' ||articles.slug group by articles.title
            order by count(*) desc limit 3;
            """
    cursor.execute(popular)
    #  Executing the query for 1st question
    r = cursor.fetchall()
    print("Most popular three articles of all time:")
    print("=============================================")
    for r in r:
        print('"{title}" - {count} views'.format(title=r[0], count=r[1]))
        ''' format identifiers help if we wantt to provide non-default
            formatting for our values.'''
    #  Query for t  he most popular author of all time
    popular = """
                select authors.name,count(name) from log,articles,authors
                where log.path = '/article/' ||articles.slug and
                articles.author=authors.id group by authors.name order by
                count(name) desc;
                """
    cursor.execute(popular)
    r = cursor.fetchall()
    print("\n")
    print("The most popular authors of all time")
    print("=============================================")
    for r in r:
        print('{author} - {count} views'.format(author=r[0], count=r[1]))

    #  Query for a day which has more than 1% of requests are error
    ''' 'WITH' clause lets you store the result of a query in a
      temporary table using an alias'''
    #  per_day_requests - It is to store how many requests are doing per day
    #  per_day_error - It is to store how many requests are errors per day
    #  count_rate_error - It is to store the rate of error for a particular day
    popular = """
              with per_day_requests as(
              select time::date as day, count(*)
              from log
              group by time::date
              order by time::date
              ), per_day_error as(
              select time::date as day, count(*)
              from log
              where status != '200 OK'
              group by time::date
              order by time::date
              ), count_rate_error as(
              select per_day_requests.day,
               per_day_error.count::float/per_day_requests.count::float*100
               as rate_error_day
              from per_day_requests, per_day_error
              where per_day_requests.day = per_day_error.day
              )
              select * from count_rate_error where rate_error_day > 1;
            """
    cursor.execute(popular)
    results = cursor.fetchall()
    print("\n")
    print("Days with more than 1% of requests leads to errors")
    print('=============================================')
    for result in results:
        print('{date:%B %d, %Y} - {count_rate_error:.1f}% errors'
              .format(date=result[0], count_rate_error=result[1]))
    '''  format identifiers help if you want to provide non-default formatting
    for your values.'''
    '''  for displaying the date '2016-07-17' in the form of july 17,2016
    we need to use the %B-month %D-date %Y-year'''
    db.close()
    # After opening database we've to close that database


if __name__ == "__main__":
    main()
