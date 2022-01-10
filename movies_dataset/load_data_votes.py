sql_to_movie_numeric_votes = """
    INSERT INTO movie_numeric_votes (
        movie_id,
        rating_id,
        vote_count)
    VALUES(
        %s,
        %s,
        %s)
    ON CONFLICT (movie_id, rating_id) DO UPDATE
    SET
        vote_count = excluded.vote_count,
        last_updated_date = now()
"""

sql_to_movie_avg_votes = """
    INSERT INTO movie_avg_votes (
        movie_id,
        rating_id,
        vote_avg,
        vote_count)
    VALUES(
        %s,
        %s,
        %s,
        %s)
    ON CONFLICT (movie_id, rating_id) DO UPDATE
    SET
        vote_avg = excluded.vote_avg,
        vote_count = excluded.vote_count
"""

def upsert_ratings(row, cur):
    from math import ceil 
    # get movie id as FK   
    cur.execute("""SELECT lookup_code, lookup_dtl_id 
                    FROM lookup_dtl 
                    WHERE lookup_hdr_id = (
                    SELECT lookup_hdr_id 
                    FROM lookup_hdr 
                    WHERE lower(lookup_type) = lower(%s))""", 
                    ("rating",)
                )

    rating_lookup_dict = dict(cur.fetchall())
    
    cur.execute("SELECT title_id, movie_id FROM movies")
    movie_dict = dict(cur.fetchall())

    movie_id = movie_dict.get(row["imdb_title_id"].lower())
    
    # insert into movie_numeric_votes
    cur.execute(sql_to_movie_numeric_votes, (movie_id, rating_lookup_dict.get("10"), row["votes_10"]))
    cur.execute(sql_to_movie_numeric_votes, (movie_id, rating_lookup_dict.get("9"), row["votes_9"]))
    cur.execute(sql_to_movie_numeric_votes, (movie_id, rating_lookup_dict.get("8"), row["votes_8"]))
    cur.execute(sql_to_movie_numeric_votes, (movie_id, rating_lookup_dict.get("7"), row["votes_7"]))
    cur.execute(sql_to_movie_numeric_votes, (movie_id, rating_lookup_dict.get("6"), row["votes_6"]))
    cur.execute(sql_to_movie_numeric_votes, (movie_id, rating_lookup_dict.get("5"), row["votes_5"]))
    cur.execute(sql_to_movie_numeric_votes, (movie_id, rating_lookup_dict.get("4"), row["votes_4"]))
    cur.execute(sql_to_movie_numeric_votes, (movie_id, rating_lookup_dict.get("3"), row["votes_3"]))
    cur.execute(sql_to_movie_numeric_votes, (movie_id, rating_lookup_dict.get("2"), row["votes_2"]))
    cur.execute(sql_to_movie_numeric_votes, (movie_id, rating_lookup_dict.get("1"), row["votes_1"]))
    
    # insert into movie_avg_votes
    cur.execute(sql_to_movie_avg_votes, (movie_id, rating_lookup_dict.get("ALLGENDERS_0AGE"), 
                                             row["allgenders_0age_avg_vote"], ceil(float(row["allgenders_0age_votes"]))))
    cur.execute(sql_to_movie_avg_votes, (movie_id, rating_lookup_dict.get("ALLGENDERS_18AGE"), 
                                             row["allgenders_18age_avg_vote"], ceil(float(row["allgenders_18age_votes"]))))
    cur.execute(sql_to_movie_avg_votes, (movie_id, rating_lookup_dict.get("ALLGENDERS_30AGE"), 
                                             row["allgenders_30age_avg_vote"], ceil(float(row["allgenders_30age_votes"]))))
    cur.execute(sql_to_movie_avg_votes, (movie_id, rating_lookup_dict.get("ALLGENDERS_45AGE"), 
                                             row["allgenders_45age_avg_vote"], ceil(float(row["allgenders_45age_votes"]))))
    cur.execute(sql_to_movie_avg_votes, (movie_id, rating_lookup_dict.get("FEMALES_0AGE"), 
                                             row["females_0age_avg_vote"], ceil(float(row["females_0age_votes"]))))
    cur.execute(sql_to_movie_avg_votes, (movie_id, rating_lookup_dict.get("FEMALES_18AGE"), 
                                             row["females_18age_avg_vote"], ceil(float(row["females_18age_votes"]))))
    cur.execute(sql_to_movie_avg_votes, (movie_id, rating_lookup_dict.get("FEMALES_30AGE"), 
                                             row["females_30age_avg_vote"], ceil(float(row["females_30age_votes"]))))
    cur.execute(sql_to_movie_avg_votes, (movie_id, rating_lookup_dict.get("FEMALES_45AGE"), 
                                             row["females_45age_avg_vote"], ceil(float(row["females_45age_votes"]))))
    cur.execute(sql_to_movie_avg_votes, (movie_id, rating_lookup_dict.get("FEMALES_ALLAGES"), 
                                             row["females_allages_avg_vote"], ceil(float(row["females_allages_votes"]))))
    cur.execute(sql_to_movie_avg_votes, (movie_id, rating_lookup_dict.get("MALES_0AGE"), 
                                             row["males_0age_avg_vote"], ceil(float(row["males_0age_votes"]))))
    cur.execute(sql_to_movie_avg_votes, (movie_id, rating_lookup_dict.get("MALES_18AGE"), 
                                             row["males_18age_avg_vote"], ceil(float(row["males_18age_votes"]))))
    cur.execute(sql_to_movie_avg_votes, (movie_id, rating_lookup_dict.get("MALES_30AGE"), 
                                             row["males_30age_avg_vote"], ceil(float(row["males_30age_votes"]))))
    cur.execute(sql_to_movie_avg_votes, (movie_id, rating_lookup_dict.get("MALES_45AGE"), 
                                             row["males_45age_avg_vote"], ceil(float(row["males_45age_votes"]))))
    cur.execute(sql_to_movie_avg_votes, (movie_id, rating_lookup_dict.get("MALES_ALLAGES"), 
                                             row["males_allages_avg_vote"], ceil(float(row["males_allages_votes"]))))
    cur.execute(sql_to_movie_avg_votes, (movie_id, rating_lookup_dict.get("TOP1000"), 
                                             row["top1000_voters_rating"], ceil(float(row["top1000_voters_votes"]))))
    cur.execute(sql_to_movie_avg_votes, (movie_id, rating_lookup_dict.get("NON_US"), 
                                             row["non_us_voters_rating"], ceil(float(row["non_us_voters_votes"]))))
    cur.execute(sql_to_movie_avg_votes, (movie_id, rating_lookup_dict.get("US"), 
                                             row["us_voters_rating"], ceil(float(row["us_voters_votes"]))))

def load_data(df):
    import sqlalchemy

    pwd = "SpiderMonkey6."
    
    engine = sqlalchemy.create_engine(f'postgresql://postgres:{pwd}@34.145.47.78:5432/postgres')
    engine.raw_connection().set_session(autocommit=True)

    cur = engine.raw_connection().cursor()

    for idx, row in df.iterrows():
        
        upsert_ratings(row, cur)


def etl_process():
    import pandas as pd 
    import numpy as np
    from multiprocessing import Pool, cpu_count

    try: 
        connect_database()

        ratings = pd.read_sql("stg_ratings", engine)
        ratings.fillna(0, inplace=True)

        no_of_parallel = cpu_count()

        df_chunks = np.array_split(ratings, no_of_parallel)

        with Pool(no_of_parallel) as p:
            p.map(load_data, df_chunks)

    except Exception as e:
        print(e)


def connect_database():
    import sqlalchemy
    import psycopg2

    pwd = "SpiderMonkey6."
    
    engine = sqlalchemy.create_engine(f'postgresql://postgres:{pwd}@34.145.47.78:5432/postgres')
    engine.raw_connection().set_session(autocommit=True)

    cur = engine.raw_connection().cursor()


if __name__ == '__main__':
    etl_process()
