from moztelemetry.hbase import HBaseMainSummaryView
from collections import Counter, namedtuple
from pyspark.sql import Row
from datetime import datetime, timedelta
import uuid
from datetime import date


def main(sc, sqlContext, save=True):
    # Load testpilot data
    sqlContext.read.parquet("s3://telemetry-parquet/harter/cliqz_testpilot/v1/")\
        .createOrReplaceTempView('cliqz_testpilot')
    sqlContext.read.parquet("s3://telemetry-parquet/harter/cliqz_testpilottest/v1/")\
        .createOrReplaceTempView('cliqz_testpilottest')

    earliest_ping_per_client = sqlContext.sql("""
        SELECT tp.client_id, min(date) as min_date
        FROM cliqz_testpilot tp
        JOIN cliqz_testpilottest tpt
        ON tpt.client_id = tp.client_id
        GROUP BY 1
        """)

    txp_data = sqlContext.sql("""
        SELECT 
            tp.client_id,
            tpt.cliqz_client_id,
            tp.submission as submission_date,
            tp.cliqz_version,
            tp.has_addon,
            tp.cliqz_version,
            tpt.event,
            tp.event as tp_event,
            tpt.content_search_engine
        FROM cliqz_testpilot tp
        JOIN cliqz_testpilottest tpt
        ON tpt.client_id = tp.client_id
        AND tpt.submission == tp.submission
        """)

    client_ids = earliest_ping_per_client\
        .rdd.map(lambda x: str(x.client_id)).distinct().take(10)#.collect()

    ms_list = get_all_ms_data(sc, client_ids)

    # Reduce the main_summary data to recent pings, where "recent" includes
    # all pings send after the start of the experiment or within two weeks
    # of the start of the experiment.
    filtered_ms = txp_min.rdd\
        .map(lambda x: (x.client_id, x))\
        .join(ms_list)\
        .flatMap(filter_and_flatten)
    
    aggregated_ms = filtered_ms.map(prep_ms_agg).reduceByKey(agg_func)
    aggregated_txp = txp.rdd.map(prep_txp_agg).reduceByKey(agg_func)

    # Join aggregated tables
    joined = agg_ms.fullOuterJoin(agg_txp)
    flattened = joined.map(format_row)

    final = sqlContext.createDataFrame(flattened)

    if save:
        final.write.mode("overwrite")\
            .parquet("s3n://telemetry-parquet/harter/cliqz_profile_daily/v2/")

    return final


def is_valid_client_id(client_id):
    """Tests whether a client_id meet's hbase's requirements for a UUID"""
    try:
        uuid.UUID(client_id)
    except:
        return False
    
    return True


def filter_ms_payload(row):
    """Takes an hbase main_summary row and filters to interesting fields"""
    fields = [
        'submission_date',
        'normalized_channel',
        'os',
        'is_default_browser',
        'subsession_length',
        'default_search_engine',
        'search_counts',
    ]
    
    addons = map(lambda x: x['addon_id'],
                 row.get('active_addons', []))

    return dict([(key, row.get(key)) for key in fields] +
                [("has_addon", "testpilot@cliqz.com" in addons)])


def read_ms_data(sc, clients):
    """Reads main_summary data from hbase and returns filtered pings"""


def get_all_ms_data(sc, client_ids):
    """cleans a list of client_ids and draws main_summary data from hbase"""
    clean_client_ids = filter(is_valid_client_id, client_ids)
    
    main_summary_data = HBaseMainSummaryView().get_range(
        sc,
        clean_client_ids,
        range_start=date(2017,1,1),
        range_end=date.today(),
        limit=1000
    ).map(lambda (k, v): (k, map(filter_ms_payload, v)))
    
    return sc.parallelize(main_summary_data)


def filter_and_flatten(row):
    """Filter ms_array to rows from no earlier than 2 weeks before expt start
    
    row: (client_id, (txp_min(client_id, min_date),
                      [ms_row_dicts]))
    
    returns: filtered dicts from main_summary (including client_id)
    """
    
    min_date = datetime.strptime(row[1][0].min_date, "%Y%m%d")
    def is_ms_row_recent(ms_row):
        try:
            submission_date = datetime.strptime(ms_row['submission_date'], "%Y%m%d")
            return (min_date - submission_date) <= timedelta(14)
        except:
            return False
    
    filtered = filter(is_ms_row_recent, row[1][1])
    
    # Add the client_id to the filtered rows:
    return map(lambda ms_dict: dict(ms_dict.items() + [('client_id', row[0])]),
               filtered)


AggRow = namedtuple("AggRow", ['raw_row', 'agg_field'])

def agg_func(x, y):
    return x[0], x[1] + y[1]


def prep_ms_agg(row):
    """Prepare main_summary data to be merged with textpilot data

    The merge will be keyed by (client_id, submission_date).
    
    row: a row of main_summary data
    returns: A tuple with the following form:
        (merge_key, AggRow)
    """
    def parse_search_counts(search_counts):
        if search_counts is not None:
            return Counter({(xx['engine'] + "-" + xx['source']): xx['count'] for xx in search_counts})
        else:
            return Counter()

    return ((row['client_id'], row['submission_date']),
        AggRow(
            raw_row = row,
            agg_field = Counter({
                "is_default_browser_counter": Counter([row['is_default_browser']]),
                "session_hours": float(row['subsession_length'] if row['subsession_length'] else 0)/3600,
                "search_counts": parse_search_counts(row['search_counts']),
                "has_addon": row['has_addon']
            })
        )
    )


def prep_txp_agg(row):
    """Prepare testpilot data to be merged with main_summary data

    The merge will be keyed by (client_id, submission_date).
    
    row: a row of testpilot data
    returns: A tuple with the following form:
        (merge_key, AggRow)
    """
    return ((row.client_id, row.submission_date),
        AggRow(
            raw_row = row,
            agg_field = Counter({
                "cliqz_enabled": int(row.tp_event == "enabled"),
                "cliqz_enabled": int(row.tp_event == "disabled"),
                "test_enabled": int(row.event == "cliqzEnabled"),
                "test_disabled": int(row.event == "cliqzDisabled"),
                "test_installed": int(row.event == "cliqzInstalled"),
                "test_uninstalled": int(row.event == "cliqzUninstalled")
            })
        )
    )


def option(value):
    """A poor man's Optional data type
    
    Returns a function that checks whether value is none before applying a
    user defined function.
    """
    return lambda func: func(value) if value is not None else None


def format_row(row):
    key, value = row
    
    # Unfortunately, the named tuple labels aren't preserved in spark, 
    # unpacking the merged values:
    main_summary, ms_agg = value[0] if value[0] is not None else (None, None)
    testpilot, txp_agg = value[1] if value[1] is not None else (None, None)

    if_ms = option(main_summary)
    if_ms_agg = option(ms_agg)
    if_txp = option(testpilot)
    if_txp_agg = option(txp_agg)

    search_counts = if_ms_agg(lambda x:x['search_counts'])
    
    return Row(
        client_id = key[0],
        cliqz_client_id = if_txp(lambda x: x.cliqz_client_id),
        date = key[1],
        has_cliqz = if_ms_agg(lambda x: bool(x['has_addon'])),
        cliqz_version = if_txp(lambda x: x.cliqz_version),
        channel = if_main_summary(lambda x: x['normalized_channel']),
        os = if_main_summary(lambda x: x['os']),
        is_default_browser = if_ms_agg(lambda x: bool(x['is_default_browser_counter'].most_common()[0][0])),
        session_hours = if_ms_agg(lambda x: x['session_hours']),
        search_default = if_main_summary(lambda x: x['default_search_engine']),
        search_counts = dict(search_counts) if search_counts is not None else {},
        cliqz_enabled = if_txp_agg(lambda x: x['cliqz_enabled']),
        cliqz_disabled = if_txp_agg(lambda x: x['cliqz_enabled']),
        test_enabled = if_txp_agg(lambda x: x['test_enabled']),
        test_disabled = if_txp_agg(lambda x: x['test_disabled']),
        test_installed = if_txp_agg(lambda x: x['test_installed']),
        test_uninstalled = if_txp_agg(lambda x: x['test_uninstalled'])
    )
