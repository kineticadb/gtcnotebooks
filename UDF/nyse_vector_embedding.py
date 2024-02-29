#!/usr/bin/env python
import gpudb
import json
from kinetica_proc import ProcData
import math
import pycatch22
import time


def c22(ts_data: list) -> list:
    timeseries = [pycatch22.CO_f1ecac(ts_data), pycatch22.CO_trev_1_num(ts_data), pycatch22.CO_FirstMin_ac(ts_data),
                  pycatch22.CO_HistogramAMI_even_2_5(ts_data), pycatch22.DN_Mean(ts_data),
                  pycatch22.DN_Spread_Std(ts_data),
                  pycatch22.DN_HistogramMode_5(ts_data), pycatch22.DN_HistogramMode_10(ts_data),
                  pycatch22.DN_OutlierInclude_n_001_mdrmd(ts_data), pycatch22.SB_BinaryStats_diff_longstretch0(ts_data),
                  pycatch22.SB_BinaryStats_mean_longstretch1(ts_data), pycatch22.SB_MotifThree_quantile_hh(ts_data),
                  pycatch22.SB_TransitionMatrix_3ac_sumdiagcov(ts_data),
                  pycatch22.SC_FluctAnal_2_dfa_50_1_2_logi_prop_r1(ts_data),
                  pycatch22.SC_FluctAnal_2_rsrangefit_50_1_logi_prop_r1(ts_data),
                  pycatch22.SP_Summaries_welch_rect_area_5_1(ts_data),
                  pycatch22.SP_Summaries_welch_rect_centroid(ts_data),
                  pycatch22.FC_LocalSimple_mean1_tauresrat(ts_data), pycatch22.FC_LocalSimple_mean3_stderr(ts_data),
                  pycatch22.IN_AutoMutualInfoStats_40_gaussian_fmmi(ts_data), pycatch22.MD_hrv_classic_pnn40(ts_data),
                  pycatch22.PD_PeriodicityWang_th0_01(ts_data)]

    return timeseries


if __name__ == '__main__':
    proc_data = ProcData()
    db = gpudb.GPUdb(host=proc_data.request_info['head_url'],
                     username=proc_data.request_info['username'],
                     password=proc_data.request_info['password'])

    if not db.has_table(f"nyse.vector")["table_exists"]:
        db.execute_sql('''create table nyse.vector
                              (
                                  ts_bkt datetime,
                                  symbol,
                                  ap_vec vector(22)
                              )''')

    while True:
        db.execute_sql('refresh materialized view nyse.prices_delta;')
        has_more_records = True
        offset = 0
        flat_result = {}

        while has_more_records:
            result = None
            try:
                result = db.execute_sql('''select
                                time_bucket(interval 5 minute, t) as ts_bkt,
                                s,
                                ap,
                                "as"
                            from nyse.prices_delta
                            where
                                ap is not null;''', encoding='json', offset=offset)

            except gpudb.GPUdbException as gpudberror:
                print(str(gpudberror))

            if result is None:
                break

            records = json.loads(result.json_encoded_response)
            has_more_records = result.has_more_records
            offset += len(records['column_1'])

            for count in range(len(records['column_1'])):
                ts_bkt = records['column_1'][count]
                symbol = records['column_2'][count]
                price = records['column_3'][count]

                if ts_bkt not in flat_result:
                    flat_result[ts_bkt] = {}

                if symbol not in flat_result[ts_bkt]:
                    flat_result[ts_bkt][symbol] = []

                flat_result[ts_bkt][symbol].append(price)

        for k1, v1 in flat_result.items():
            for k2, v2 in v1.items():
                if len(v2) > 10:
                    vec = c22(v2)
                    vec = [0 if math.isnan(x) else x for x in vec]

                    try:
                        response = db.insert_records_from_json(
                            table_name='nyse.vector',
                            json_records=json.dumps({'ts_bkt': k1,
                                                     'symbol': k2,
                                                     'ap_vec': vec
                                                     }))

                    except gpudb.GPUdbException as gpudberror:
                        print(str(gpudberror))
        print()
        time.sleep(60 * 5)

    proc_data.complete()
