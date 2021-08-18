from mongo_check import Check_Mongo
import time
import pandas as pd
from td import last_tradeday
import sys


if __name__ == '__main__':
    start = time.time()
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.width', 200)
    pd.set_option('mode.chained_assignment', None)

    if len(sys.argv) == 1:
        get_date = last_tradeday()
    else:
        get_date = sys.argv[1]
    # 或者直接输入想查询的日期 get_date = "20210715"

    daily_full_list = ["ts_stock_basic", 'ts_daily_adj_factor', 'wind_financial_2014', 'wind_financial_wsd',
                       "wind_future_info", "wind_future",
                       "wind_option_info", "wind_option_day",
                       "wind_index_option_info", "wind_index_option_day",
                       "wind_ftsidx_info", "wind_ftsidx_day",
                       "wind_cbond_basic", "wind_cbond_day",
                       "ts_fund_info", "ts_fund_day",
                       "wind_pfund_info", "wind_pfund_day",
                       "wind_index", "wind_hsc_summary_day"]

    daily_list = ['ts_daily_adj_factor',
                  'wind_financial_2014',
                  'wind_financial_wsd',
                  "wind_future",
                  "wind_option_day",
                  "wind_index_option_day",
                  "wind_ftsidx_day",
                  "wind_cbond_day",
                  "ts_fund_day",
                  "wind_pfund_day",
                  "wind_index",
                  "jq_future_mint", "wind_future_mint",
                  "wind_option_mint",
                  "wind_index_option_mint",
                  "wind_financial_q_data",
                  "wind_hsc_summary_day"
                  ]

    check = Check_Mongo(server="192.168.1.119", port=27017, user='user', passwd='user321', table_list=daily_list, date=get_date)
    check.loop_table()
    end = time.time()
    print("time:", end - start)
