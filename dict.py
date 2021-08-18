import json


# ts_fund_day 暂时不检查数据完整性 只有三只

table_dict = {"check_data_full_stock": ['ts_daily_adj_factor', 'wind_financial_2014', 'wind_financial_wsd'],
              "check_data_full_not_stock": ["wind_future", "wind_option_day", "wind_index_option_day", "wind_ftsidx_day",
                                            "wind_cbond_day", 'wind_pfund_day'],
              "check_data_full_dict": ["ts_fund_day", 'wind_index'],  # 按照固定list检查
              "check_data_full_mint": ["jq_future_mint", "wind_future_mint", 'wind_index_option_mint', 'wind_option_mint'],  # 日频数据的检查
              "check_data_full_qdata": ["wind_financial_q_data"],  # 查看财报数据的更新情况  根据报告日和发行日
              "check_data_not_null": ['ts_daily_adj_factor', 'wind_financial_2014', 'wind_financial_wsd', "wind_option_day",
                                      "wind_index_option_day", "wind_ftsidx_day",  "ts_fund_day",
                                      'wind_pfund_day', 'wind_index',
                                      # mint
                                      'wind_index_option_mint', 'wind_option_mint'],
              "check_data_not_null_cbond": ["wind_cbond_day"],
              "check_data_not_null_future": ["wind_future", "jq_future_mint", "wind_future_mint"],
              "check_data_not_negative": ['ts_daily_adj_factor', 'wind_financial_2014', 'wind_financial_wsd', "wind_future", "wind_option_day",
                                          "wind_index_option_day", "wind_ftsidx_day", "wind_cbond_day", "ts_fund_day",
                                          'wind_pfund_day', 'wind_index',
                                          # mint
                                          "jq_future_mint", "wind_future_mint", 'wind_index_option_mint', 'wind_option_mint'],
              "check_data_positive": ['ts_daily_adj_factor', 'wind_financial_2014', 'wind_financial_wsd', "wind_future", "wind_option_day",
                                      "wind_index_option_day", "wind_ftsidx_day", "wind_cbond_day", "ts_fund_day",
                                      'wind_pfund_day', 'wind_index',
                                      # mint
                                      "jq_future_mint", "wind_future_mint", 'wind_index_option_mint', 'wind_option_mint'],

              "check_north_capital": ["wind_hsc_summary_day"],
              }


can_not_null = {"ts_daily_adj_factor": ['amount', 'change', 'close', 'code', 'date', 'high', 'low', 'open',
                                        'pre_close', 'volume', 'adj_factor', 'limit_down', 'limit_up'],
                'wind_financial_2014': ['code', 'date', 'total_shares', 'free_float_shares', 'mkt_cap_ard', 'pb_lf', 'pe_ttm'],
                'wind_financial_wsd': ['code', 'date', 'total_shares', 'free_float_shares', 'mkt_cap_ard', 'pb_lf', 'pe_ttm'],
                "wind_future": ['code', 'date', 'close', 'high', 'low', 'open',
                                'prev_close', 'volume', 'total_turnover', 'limit_down', 'limit_up',
                                "open_interest", "long_margin_rate", "short_margin_rate", "prev_settlement", "settlement"],
                "wind_option_day": ['code', 'date', 'close', 'high', 'low', 'open',
                                    'prev_close', 'volume', 'total_turnover', 'limit_down', 'limit_up',
                                    "open_interest", "prev_settlement", "settlement",  "_us_code", "contract_multiplier",
                                    "delta", "gamma", "imp_volatility", "iopv", "rho", "strike_price", "theta", "vega"],
                "wind_index_option_day": ['code', 'date', 'close', 'high', 'low', 'open',
                                          'prev_close', 'volume', 'total_turnover',
                                          "open_interest", "prev_settlement", "settlement", "contract_multiplier",
                                          "delta", "gamma", "imp_volatility", "iopv", "rho", "strike_price", "theta", "vega"],
                "wind_ftsidx_day": ['code', 'date', 'close', 'high', 'low', 'open', 'prev_close', 'volume', "open_interest"],

                "wind_cbond_day":  ['code', 'date', 'close', 'high', 'low', 'open', 'volume', 'amount', 'pct_chg',
                                    'trade_status', 'last_balance', 'rate_issuer', 'rate_bond',
                                    '_end_update', 'amount_ma12', 'avg_price'],


                "ts_fund_day": ['close', 'code', 'date', 'high', 'low', 'open',
                                'prev_close', 'volume', 'adj_factor', 'total_turnover'],
                'wind_pfund_day': ['code', 'date', 'nav', 'adj_factor', 'netasset_total'],  # 'nav_adj_return1'
                'wind_index': ['code', 'date', 'close', 'high', 'low', 'open',
                               'volume', 'amount', 'turn_rate', 'mkt_cap_ard', 'dividendyield2',
                               # turn_rate 换手率  mkt_cap_ard 总市值 总股本*股价 dividendyield 股息
                               'pe_ttm', 'pb_lf'],
                               # 滚动市盈率 市净率 peg 市盈率相对盈利增长比率(可以为负)
                # mint
                "wind_future_mint": ['code', 'date', 'datetime', 'close', 'high', 'low', 'open',
                                     "cum_open", "cum_high", "cum_low", 'prev_close', "open_interest",
                                     'volume', 'total_turnover', 'cum_volume', 'cum_turnover', 'limit_down', 'limit_up',
                                     "prev_settlement"],
                "jq_future_mint": ['code', 'date', 'datetime', 'close', 'high', 'low', 'open',
                                   "cum_open", "cum_high", "cum_low", 'prev_close', "open_interest",
                                   'volume', 'total_turnover', 'cum_volume', 'cum_turnover', 'limit_down', 'limit_up',
                                   "prev_settlement"],
                "wind_option_mint": ['code', 'date', 'datetime', 'close', 'high', 'low', 'open',
                                     "cum_open", "cum_high", "cum_low", 'prev_close', "open_interest",
                                     'volume', 'total_turnover', 'cum_volume', 'cum_turnover', "prev_settlement"],
                "wind_index_option_mint": ['code', 'date', 'datetime', 'close', 'high', 'low', 'open',
                                           "cum_open", "cum_high", "cum_low", 'prev_close', "open_interest",
                                           'volume', 'total_turnover', 'cum_volume', 'cum_turnover', "prev_settlement"],


                }


# wind_future
# open_interest 未平仓量
# volume 交易量
# total_turnover 总成交额
# long_margin_rate 保证金率
# prev_settlement 前一日结算价
# settlement 结算价

# option
# contract_multiplier 合约乘数
# Delta是期权价格对标的资产价格的敏感度，而Gamma是Delta对标的价格的敏感度。
# ETF参考单位基金净值 (Indicative Optimized．Portfolio Value，IOPV)

# pfund
# nav 单位净值


can_not_negative = {'ts_daily_adj_factor': ["amount", "close", "high", "low", "open", "pre_close", "volume",
                                            "adj_factor", "limit_down", "limit_up"],
                    'wind_financial_2014': ['total_shares', 'free_float_shares', 'mkt_cap_ard'],
                    'wind_financial_wsd': ['total_shares', 'free_float_shares', 'mkt_cap_ard'],
                    "wind_future": ['close', 'high', 'low', 'open',
                                    'prev_close', 'volume', 'total_turnover', 'limit_down', 'limit_up',
                                    "open_interest", "long_margin_rate", "short_margin_rate", "prev_settlement", "settlement"],
                    "wind_option_day": ['close', 'high', 'low', 'open', 'prev_close', 'volume', 'total_turnover',
                                        'limit_down', 'limit_up', "prev_settlement", "settlement",  "open_interest",
                                        "contract_multiplier", "strike_price", "imp_volatility", "iopv",
                                        "vega", "gamma"],
                    "wind_index_option_day": ['close', 'high', 'low', 'open', 'prev_close', 'volume', 'total_turnover',
                                              "prev_settlement", "settlement", "open_interest",
                                              "contract_multiplier", "strike_price", "imp_volatility", "iopv",
                                              "vega", "gamma"],
                    "wind_ftsidx_day": ['close', 'high', 'low', 'open', 'prev_close', 'volume', "open_interest"],
                    "wind_cbond_day": ['close', 'high', 'low', 'open', 'volume', 'amount',
                                       'last_balance', 'amount_ma12', 'avg_price'],
                    "ts_fund_day": ['close', 'high', 'low', 'open',
                                    'prev_close', 'volume', 'adj_factor', 'total_turnover'],
                    'wind_pfund_day': ['nav', 'adj_factor', 'netasset_total'],
                    'wind_index': ['close', 'high', 'low', 'open',
                                   'volume', 'amount', 'turn_rate', 'mkt_cap_ard', 'dividendyield2',
                                   'pe_ttm', 'pb_lf'],
                    # mint
                    "wind_future_mint": ['close', 'high', 'low', 'open',
                                         "cum_open", "cum_high", "cum_low", 'prev_close', "open_interest",
                                         'volume', 'total_turnover', 'cum_volume', 'cum_turnover', 'limit_down', 'limit_up',
                                         "prev_settlement"],
                    "jq_future_mint":  ['close', 'high', 'low', 'open',
                                        "cum_open", "cum_high", "cum_low", 'prev_close', "open_interest",
                                        'volume', 'total_turnover', 'cum_volume', 'cum_turnover', 'limit_down', 'limit_up',
                                        "prev_settlement"],
                    "wind_option_mint": ['close', 'high', 'low', 'open',
                                         "cum_open", "cum_high", "cum_low", 'prev_close', "open_interest",
                                         'volume', 'total_turnover', 'cum_volume', 'cum_turnover', "prev_settlement"],
                    "wind_index_option_mint": ['close', 'high', 'low', 'open',
                                               "cum_open", "cum_high", "cum_low", 'prev_close', "open_interest",
                                               'volume', 'total_turnover', 'cum_volume', 'cum_turnover', "prev_settlement"]

                    }

must_positive = {'ts_daily_adj_factor': ["close", "high", "low", "open", "pre_close",
                                         "adj_factor", "limit_down", "limit_up"],
                 'wind_financial_2014': ['total_shares', 'free_float_shares', 'mkt_cap_ard'],
                 'wind_financial_wsd': ['total_shares', 'free_float_shares', 'mkt_cap_ard'],
                 "wind_future": ['close', 'high', 'low', 'open', 'prev_close',  'limit_down', 'limit_up',
                                 "long_margin_rate", "short_margin_rate", "prev_settlement", "settlement"],
                 "wind_option_day": ['close', 'high', 'low', 'open', 'prev_close', 'limit_down', 'limit_up',
                                     "prev_settlement", "settlement", "contract_multiplier", "strike_price"],
                 "wind_index_option_day": ['close', 'high', 'low', 'open', 'prev_close', "prev_settlement",
                                           "settlement", "contract_multiplier", "strike_price"],
                 "wind_ftsidx_day": ['close', 'high', 'low', 'open', 'prev_close'],
                 "wind_cbond_day": ['close', 'high', 'low', 'open', 'avg_price'],
                 "ts_fund_day": ['close', 'high', 'low', 'open', 'prev_close', 'adj_factor'],
                 'wind_pfund_day': ['nav', 'adj_factor', 'netasset_total'],
                 'wind_index': ['close', 'high', 'low', 'open', 'turn_rate', 'mkt_cap_ard', 'dividendyield2',
                                'pe_ttm', 'pb_lf'],
                 # mint
                 "wind_future_mint": ['close', 'high', 'low', 'open',
                                      "cum_open", "cum_high", "cum_low", 'prev_close', 'limit_down', 'limit_up',
                                      "prev_settlement"],
                 "jq_future_mint": ['close', 'high', 'low', 'open',
                                    "cum_open", "cum_high", "cum_low", 'prev_close', 'limit_down', 'limit_up',
                                    "prev_settlement"],
                 "wind_option_mint": ['close', 'high', 'low', 'open',
                                      "cum_open", "cum_high", "cum_low", 'prev_close', "prev_settlement"],
                 "wind_index_option_mint": ['close', 'high', 'low', 'open',
                                            "cum_open", "cum_high", "cum_low", 'prev_close', "prev_settlement"],
                 }


wind_index_list = [
        '000001.SH', '000016.SH', '000300.SH', '000905.SH', '000906.SH', '000852.SH',
        '399001.SZ', '399006.SZ',
        '881001.WI', '801811.SI', '801812.SI', '801813.SI',
        "CI005918.WI", "CI005919.WI",
        # 中信一级行业
        'CI005001.WI', 'CI005002.WI', 'CI005003.WI', 'CI005004.WI', 'CI005005.WI',
        'CI005006.WI', 'CI005007.WI', 'CI005008.WI', 'CI005009.WI', 'CI005010.WI',
        'CI005011.WI', 'CI005012.WI', 'CI005013.WI', 'CI005014.WI', 'CI005015.WI',
        'CI005016.WI', 'CI005017.WI', 'CI005018.WI', 'CI005019.WI', 'CI005020.WI',
        'CI005021.WI', 'CI005022.WI', 'CI005023.WI', 'CI005024.WI', 'CI005025.WI',
        'CI005026.WI', 'CI005027.WI', 'CI005028.WI', 'CI005029.WI', 'CI005030.WI',
        # 中信二级行业
        'CI005101.WI', 'CI005102.WI', 'CI005104.WI', 'CI005105.WI',
        'CI005106.WI', 'CI005107.WI', 'CI005109.WI', 'CI005110.WI',
        'CI005111.WI', 'CI005113.WI', 'CI005117.WI', 'CI005122.WI', 'CI005124.WI',
        'CI005127.WI', 'CI005129.WI', 'CI005133.WI', 'CI005134.WI', 'CI005135.WI',
        'CI005136.WI', 'CI005137.WI', 'CI005138.WI', 'CI005139.WI', 'CI005140.WI',
        'CI005143.WI', 'CI005144.WI', 'CI005145.WI', 'CI005146.WI', 'CI005152.WI',
        'CI005153.WI', 'CI005154.WI', 'CI005155.WI', 'CI005156.WI', 'CI005160.WI',
        'CI005162.WI', 'CI005163.WI', 'CI005164.WI', 'CI005165.WI',
        'CI005166.WI', 'CI005168.WI', 'CI005170.WI', 'CI005171.WI', 'CI005172.WI',
        'CI005173.WI', 'CI005178.WI', 'CI005181.WI', 'CI005185.WI', 'CI005187.WI',
        'CI005188.WI', 'CI005189.WI', 'CI005190.WI', 'CI005191.WI', 'CI005192.WI',
        'CI005193.WI', 'CI005194.WI', 'CI005195.WI', 'CI005196.WI', 'CI005197.WI',
        'CI005198.WI', 'CI005199.WI', 'CI005800.WI', 'CI005801.WI', 'CI005802.WI',
        'CI005803.WI', 'CI005804.WI', 'CI005805.WI', 'CI005806.WI', 'CI005807.WI',
        'CI005808.WI', 'CI005809.WI', 'CI005810.WI', 'CI005811.WI', 'CI005812.WI',
        'CI005813.WI', 'CI005814.WI', 'CI005815.WI', 'CI005816.WI', 'CI005817.WI',
        'CI005818.WI', 'CI005819.WI', 'CI005820.WI', 'CI005821.WI', 'CI005822.WI',
        'CI005823.WI', 'CI005824.WI', 'CI005825.WI', 'CI005826.WI', 'CI005827.WI',
        'CI005828.WI', 'CI005829.WI', 'CI005830.WI', 'CI005831.WI', 'CI005832.WI',
        'CI005833.WI', 'CI005834.WI', 'CI005835.WI', 'CI005836.WI', 'CI005837.WI',
        'CI005838.WI', 'CI005839.WI', 'CI005840.WI', 'CI005841.WI', 'CI005842.WI',
        'CI005843.WI', 'CI005844.WI', 'CI005845.WI', 'CI005846.WI', 'CI005847.WI',
        'CI005848.WI', 'CI005849.WI',
    ]

ts_fund_day_list = ["159919.SZ", '510300.SH', '510050.SH']

all_dict = {"table_dict": table_dict, "can_not_null": can_not_null, "can_not_negative": can_not_negative, "must_positive": must_positive,
            "wind_index": wind_index_list, "ts_fund_day": ts_fund_day_list}


def save_dict(filename, dic):
    '''save dict into json file'''
    with open(filename, 'w') as json_file:
        json.dump(dic, json_file, ensure_ascii=False, indent=4)


save_dict("dict_info.json", all_dict)


