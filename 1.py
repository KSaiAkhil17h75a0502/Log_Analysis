#!/usr/bin/env python3
import psycopg2
import datetime


def get_most_popular_three_articles():
    database = psycopg2.connect("dbname=news")
    #  Connecting to database
    cursor = database.cursor()
    #  Query to find the most popular three articles of all time
    query = """
            select a.title, count(l.path) as count
            from log as l
            join articles as a on ('/article/' || a.slug) = l.path
            where l.path != '/'
            group by l.path, a.title
            order by count(l.path)
            desc limit 3;
        """
    cursor.execute(query)
    #  Executing the query
    return cursor.fetchall()
    database.close()
#  Displying the output


def display_articles(data):
    articles_array = data
    for r in articles_array:
        print('"' + r[0] + '" - ' + str(r[1]) + ' views')


print ('The three most poular articles of all time')
display_articles(get_most_popular_three_articles())


def get_most_popular_article_authors():
    database = psycopg2.connect("dbname=news")
    cursor = database.cursor()
    #  Query to find most popular authors of all time
    query = """
            select aut.name, count(art.author) as count
            from log as l
            join articles as art on ('/article/' || art.slug) = l.path
            join authors as aut on art.author = aut.id
            where l.path != '/'
            group by art.author, aut.name
            order by count(art.author) desc;
        """
    cursor.execute(query)
    return cursor.fetchall()
    database.close()


def display_authors(data):
    authors_array = data
    for au in authors_array:
        print(au[0] + '  - ' + str(au[1]) + ' views ')


print ("\nThe most popular article authors of all time:")
display_authors(get_most_popular_article_authors())


def get_days_with_requests_errors():
    database = psycopg2.connect("dbname=news")
    cursor = database.cursor()
    # Query On which days did more than 1% of requests lead to errors
    query = """
              (select q1.date, ((q1.count * 100) / q2.count) as percentage
              from (select date(time) as date, count(status) as count
              from log
              where status like '%4%' or status like '%5%'
              group by date(time)) as q1,
              (select date(time) as date, count(status) as count
              from log
              group by date(time)) as q2
              where q1.date = q2.date and ((q1.count * 100) / q2.count) > 1)
        """
    cursor.execute(query)
    return cursor.fetchall()
    database.close()


def display_error_days(data):
    error_days_array = data
    for error_day in error_days_array:
        day = error_day[0].day
        month = datetime.date(1900, error_day[0].month, 1).strftime('%B')
        year = error_day[0].year
        print(month + ' ' + str(day) + ', ' + str(year) +
              ' - ' + str(error_day[1]) + '% errors')


print ("\nDays with more than 1% of requests lead to errors:")
display_error_days(get_days_with_requests_errors())
