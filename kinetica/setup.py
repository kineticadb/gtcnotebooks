import gpudb
import json


def setup():
    db = gpudb.GPUdb(host='https://demo72.kinetica.com/_gpudb',
                     username='gtc',
                     password='Kinetica123!')

    nyse_stream_proc = None
    nyse_delta_embeddings_proc = None

    if db.has_proc(proc_name='nyse_stream')['proc_exists'] is not True:
        print('need to create stream proc')
    elif db.has_proc(proc_name='nyse_delta_embeddings')['proc_exists'] is not True:
        print('need to create embeddings proc')
    else:
        for key, value in db.show_proc_status(run_id='')['proc_names'].items():
            if value == 'nyse_stream':
                nyse_stream_proc = key

            if value == 'nyse_delta_embeddings':
                nyse_delta_embeddings_proc = key

        if db.show_proc_status(run_id=str(nyse_stream_proc))["status_info"]["status"] != "OK":
            db.execute_proc(proc_name="nyse_stream")

        if db.show_proc_status(run_id=str(nyse_delta_embeddings_proc))["status_info"]["status"] != "OK":
            db.execute_proc(proc_name="nyse_delta_embeddings")

    sqlcontext = '''CREATE OR REPLACE CONTEXT nyse.nyse_vector_ctxt
        (
            TABLE = nyse.prices,
            COMMENT = 'Stock prices including ask, bid, and sale price and size',
            RULES = (
                'when I ask about stock prices, use the nyse.prices table',
                'when I ask about stock prices today, filter on all results that occurred between now and an interval of 1 day',
                'all stock symbols are in lower case',
                'when I ask about today I mean that the timestamp should be greater than or equal to now minus an interval of 1 day',
                'when I ask about any column, make sure there are no null values or NaN values',
                'replace all NaN values with 0 using the IFNAN() function',
                'all numeric results should be larger than 0',
                'convert all stock symbols to lower case',
                'always filter out null values'
            ),
            COMMENTS = (
                'ap' = 'ask price',
                'bp' = 'bid price',
                'bs' = 'bid size',
                'lp' = 'sale price',
                'ls' = 'sale size',
                's' = 'symbol',
                't' = 'timestamp'
            )
        ),
        (
            TABLE = nyse.vector,
            COMMENT = 'Time-series vector embeddings for NYSE stock characteristics'
        ),
        (
            SAMPLES = (
                'find all sofi stock trades between 2024-01-29 14:25:00 and 2024-01-29 14:35:00 where the price is not null' = 'SELECT t, s, lp
        FROM nyse.prices
        WHERE s =''sofi''
        AND t BETWEEN ''2024-01-29 14:25:00'' AND ''2024-01-29 14:35:00''
        AND lp IS NOT NULL;',
        
                'find similar patterns to sofi at 2024-01-29 14:25:00.000' = 'SELECT
            ts_bkt,
            symbol,
            dot_product(ap_vec,(select string(ap_vec) from (select * from nyse.vector where ts_bkt = ''2024-01-29 14:25:00.000'' and symbol = ''sofi'' limit 1))) as d1
        FROM
            nyse.vector
        ORDER BY
            d1 asc
        LIMIT
            5',
            
                'how many rows are in the prices data?' = 'select
          count(*)
        from
          nyse.prices',
          
                'show me buying opportunities for the next 15 min' = 'SELECT
            ts_bkt,
            symbol,
            dot_product(ap_vec,(select string(ap_vec) from (select * from nyse.vector where ts_bkt = ''2024-01-29 14:25:00.000'' and symbol = ''sofi'' limit 1))) as d1
        FROM
            nyse.vector
        WHERE
            ts_bkt >= now() - interval ''15'' minutes
        ORDER BY
            d1 asc
        LIMIT
            5',
        
                'what stock symbol other than QQQ has the highest price within the last 15 minutes?' = 'SELECT
            s
        FROM
            nyse.prices
        WHERE
            t >= now() - interval ''15'' minutes
            and s <> ''qqq''
        GROUP BY
            s
        ORDER BY
            avg(lp) desc
        LIMIT
            1;'
            )
        )
    '''
    #db.execute_sql(sqlcontext)
