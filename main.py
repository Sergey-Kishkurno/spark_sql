from datetime import datetime
from contextlib import closing
import psycopg2

from hdfs import InsecureClient
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


pg_creds = {
    'host': '0.0.0.0',
    'port': '5432',
    'database': 'postgres',
    'user': 'pguser',
    'password': 'secret'
}
pg_url = "jdbc:postgresql://0.0.0.0:5432/postgres"
pg_properties = {
    'user': 'pguser',
    'password': 'secret'
}

current_date = datetime.now().strftime("%Y-%m-%d")

client = InsecureClient('http://0.0.0.0:50070', user='user')

spark = SparkSession.builder \
    .config('spark.driver.extraClassPath'
            , '/home/user/shared_folder/postgresql-42.2.20.jar') \
    .master('local') \
    .appName("main") \
    .getOrCreate()


##### LOAD TO BRONZE ZONE #########################################################################

def bronze_upload():

    tables_to_load = (
        'actor',
        'address',
        'category',
        'city',
        'country',
        'customer',
        'film',
        'film_actor',
        'film_category',
        'inventory',
        'language',
        'rental',
        'staff',
        'store'
    )

    part_tables_to_load = (
        'payment',
    )

    dir_name = '/bronze/' + current_date
    client.makedirs(dir_name)

    for table_name in tables_to_load:
        with closing(psycopg2.connect(**pg_creds)) as pg_connection:
            cursor = pg_connection.cursor()
            file_name = dir_name + '/' + table_name + '.csv'
            with client.write(file_name, overwrite=True) as csv_file:
                cursor.copy_expert(f"COPY {table_name} TO STDOUT WITH HEADER CSV", csv_file)
        print(f"Successfully wrote file: {file_name}")

    for part_table_name in part_tables_to_load:
        with closing(psycopg2.connect(**pg_creds)) as pg_connection:
            cursor = pg_connection.cursor()
            file_name = dir_name + '/' + part_table_name + '.csv'
            with client.write(file_name, overwrite=True) as csv_file:
                cursor.copy_expert(f"COPY (SELECT * FROM {part_table_name}) TO STDOUT WITH HEADER CSV", csv_file)
        print(f"Successfully wrote file: {file_name}")


def main():

    # LOAD TO BRONZE ZONE
    # bronze_upload()

    # PROCESS AND SAVE IN SILVER ZONE
    # request_1()
    # request_2()
    # request_3()
    request_4()
    # request_5()
    # request_6()
    # request_7()


##### LOAD TO SILVER ZONE #########################################################################

# -- 1. ?????????????? ???????????????????? ?????????????? ?? ???????????? ??????????????????, ?????????????????????????? ???? ????????????????.
# select c.name, count(*) as count
# from film f
# left join film_category fc on f.film_id = fc.film_id
# left join category c on fc.category_id = c.category_id
# group by c.name
# order by count desc
# ;

def request_1():

    film_df = spark.read.load(f"/bronze/{current_date}/film.csv"
                              , header="true"
                              , inferSchema="true"
                              , format="csv")
    film_df.show()
    film_df.printSchema()

    film_category_df = spark.read.load(f"/bronze/{current_date}/film_category.csv"
                                       , header="true"
                                       , inferSchema="true"
                                       , format="csv")

    film_category_df.show()
    film_category_df.printSchema()

    category_df = spark.read.load(f"/bronze/{current_date}/category.csv"
                                  , header="true"
                                  , inferSchema="true"
                                  , format="csv")

    category_df.show()
    category_df.printSchema()

    res_df_01 = film_df.join(
        film_category_df,
        film_df['film_id'] == film_category_df['film_id'],
        'left'
    )
    res_df_01.show()
    res_df_01.printSchema()

    res_df_02 = res_df_01.join(
        category_df,
        res_df_01['category_id'] == category_df['category_id'],
        'left'
    )
    res_df_02.show()
    res_df_02.printSchema()

    res_df_03 = res_df_02.select('name')
    res_df_03.show()
    res_df_03.printSchema()

    res_df = res_df_03.groupBy('name')\
        .count()\
        .sort('count', ascending=False)

    res_df.show()



# -- 2. ?????????????? 10 ??????????????, ?????? ???????????? ???????????????? ?????????? ????????????????????, ?????????????????????????? ???? ????????????????.
# select a.first_name, a.last_name, sum(f.rental_duration) sum_rental_duration
# from actor a
#          left join film_actor fa on a.actor_id = fa.actor_id
#          left join film f on fa.film_id = f.film_id
# group by a.first_name, a.last_name
# order by sum_rental_duration desc
# limit 10
# ;


def request_2():

    film_df = spark.read.load(f"/bronze/{current_date}/film.csv"
                              , header="true"
                              , inferSchema="true"
                              , format="csv")
    film_df.show()
    film_df.printSchema()

    film_actor_df = spark.read.load(f"/bronze/{current_date}/film_actor.csv"
                                       , header="true"
                                       , inferSchema="true"
                                       , format="csv")

    film_actor_df.show()
    film_actor_df.printSchema()

    actor_df = spark.read.load(f"/bronze/{current_date}/actor.csv"
                                  , header="true"
                                  , inferSchema="true"
                                  , format="csv")

    actor_df.show()
    actor_df.printSchema()

    res_df_01 = actor_df.join(
        film_actor_df,
        actor_df['actor_id'] == film_actor_df['actor_id'],
        'left'
    )
    res_df_01.show()
    res_df_01.printSchema()

    res_df_02 = res_df_01.join(
        film_df,
        res_df_01['film_id'] == film_df['film_id'],
        'left'
    )
    res_df_02.show()
    res_df_02.printSchema()

    res_df_03 = res_df_02.select('first_name', 'last_name', 'rental_duration')
    res_df_03.show()
    res_df_03.printSchema()

    res_df_04 = res_df_03.withColumn('rental_duration', res_df_03.rental_duration.cast('int'))
    res_df_04.show()
    res_df_04.printSchema()

    res_df_05 = res_df_04.groupBy('first_name', 'last_name').sum('rental_duration')

    res_df = res_df_05.sort(F.desc('sum(rental_duration)')).limit(10)

    res_df.show()



# -- ???3
# -- ?????????????? ?????????????????? ??????????????, ???? ?????????????? ?????????????????? ???????????? ?????????? ??????????.
# select c.name, res.sum from category c
# join
#     (select fc.category_id, sum(amount)
#     from payment p
#     join rental r on p.rental_id = r.rental_id                    +
#     join inventory i on r.inventory_id =i.inventory_id            +
#     join film_category fc on fc.film_id = i.film_id               +
#     group by fc.category_id
#     order by sum desc
#     limit 1
#     ) as res on c.category_id = res.category_id
# ;

def request_3():
    category_df = spark.read.load(f"/bronze/{current_date}/category.csv"
                                  , header="true"
                                  , inferSchema="true"
                                  , format="csv")
    category_df.show()
    category_df.printSchema()

    film_category_df = spark.read.load(f"/bronze/{current_date}/film_category.csv"
                                       , header="true"
                                       , inferSchema="true"
                                       , format="csv")
    film_category_df.show()
    film_category_df.printSchema()

    payment_df = spark.read.load(f"/bronze/{current_date}/payment.csv"
                                 , header="true"
                                 , inferSchema="true"
                                 , format="csv")
    payment_df.show()
    payment_df.printSchema()

    rental_df = spark.read.load(f"/bronze/{current_date}/rental.csv"
                                , header="true"
                                , inferSchema="true"
                                , format="csv")
    rental_df.show()
    rental_df.printSchema()

    inventory_df = spark.read.load(f"/bronze/{current_date}/inventory.csv"
                                   , header="true"
                                   , inferSchema="true"
                                   , format="csv")
    inventory_df.show()
    inventory_df.printSchema()

    res_df_01 = payment_df.join(
        rental_df,
        payment_df['rental_id'] == rental_df['rental_id'],
        'left'
    )
    res_df_01.show()
    res_df_01.printSchema()

    res_df_02 = res_df_01.join(
        inventory_df,
        res_df_01['inventory_id'] == inventory_df['inventory_id'],
        'left'
    )
    res_df_02.show()
    res_df_02.printSchema()

    res_df_03 = res_df_02.join(
        film_category_df,
        res_df_02['film_id'] == film_category_df['film_id'],
        'left'
    )
    res_df_03.show()
    res_df_03.printSchema()

    res_df_04 = res_df_03.select('category_id', 'amount')

    res_df_05 = res_df_04.groupby('category_id').sum('amount')
    res_df_05.show()
    res_df_05.printSchema()

    res_df = res_df_05.sort(F.desc('sum(amount)')).limit(1)

    res_df.show()


# -- ???4
# -- ?????????????? ???????????????? ??????????????, ?????????????? ?????? ?? inventory.
# -- ???????????????? ???????????? ?????? ?????????????????????????? ?????????????????? IN.
# select title from film f
# left join inventory i on f.film_id = i.film_id
# where i.film_id is null
# ;

def request_4():
    film_df = spark.read.load(f"/bronze/{current_date}/film.csv"
                              , header="true"
                              , inferSchema="true"
                              , format="csv")
    film_df.show()
    film_df.printSchema()

    inventory_df = spark.read.load(f"/bronze/{current_date}/inventory.csv"
                                   , header="true"
                                   , inferSchema="true"
                                   , format="csv")
    inventory_df.show()
    inventory_df.printSchema()

    res_df_01 = film_df.join(
        inventory_df,
        film_df['film_id'] == inventory_df['film_id'],
        'left'
    ).select('title', inventory_df['film_id'])
    res_df_01.show()
    res_df_01.printSchema()

    res_df = res_df_01.filter(res_df_01.film_id.isNnull())

    res_df.show()




if __name__ == '__main__':
    main()