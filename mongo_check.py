from pymongo import MongoClient
import pandas as pd
import json
from collections import Counter
import sys
import re

# from bson.objectid import ObjectId


# sys.stdout = open('recode.log', mode='w', encoding='utf-8')
# 输出Log文件


class Check_Mongo(object):

    def __init__(self, server, port, user, passwd, table_list, date, filename='', charset='utf8'):
        self.server = server
        self.port = port
        self.user = user
        self.passwd = passwd
        self.charset = charset
        # self.nowtime = (datetime.datetime.now()-datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        self.nowtime = date
        self.table_list = table_list

        table_name = list(dict(table_list=table_list).keys())[0]
        filename = './check_log/' + self.nowtime + '_' + filename + '_check.log'
        sys.stdout = open(filename, mode='w', encoding='utf-8')

    def connect(self):
        client = MongoClient(self.server, self.port)
        auth = client.admin
        auth.authenticate(self.user, self.passwd)
        db = client.zcs
        return db

    def connect_news_class(self):
        client = MongoClient(self.server, self.port)
        auth = client.admin
        auth.authenticate(self.user, self.passwd)
        db = client.News_Classification
        return db

    def load_dict(self):
        with open("dict_info.json", "r") as json_file:
            dict_info = json.load(json_file)
        return dict_info

    def get_table_list(self, check_name):

        # check_update_time 是所有表都需要检查（按日更新，按月更新）
        table_dict = (self.load_dict())["table_dict"]
        if check_name == "check_update_time":
            res = self.table_list
        else:
            res = list(set(self.table_list).intersection(set(table_dict[check_name])))
        return res

    # 1.先查更新时间
    def check_update_time(self, table_name):
        # print("***********数据更新检查************")
        db = self.connect()
        table = db[table_name]
        updated_time = (table.find({}, {"_id": 1}).sort([("_id", -1)]).limit(1))[0]["_id"]
        updated_time = updated_time.generation_time.strftime('%Y-%m-%d')
        if updated_time >= self.nowtime:  # 或者==
            print("*数据更新检查：", table_name, self.nowtime, "has updated")
            return True
        else:
            print("*数据更新检查：", self.nowtime, "has not updated. Data has updated to", updated_time)
            return False

    # 2.再查更新数据完整性(针对股票数目，日行情表)
    def check_data_full_stock(self, table_name):
        db = self.connect()
        basic_dict = {"ts_daily_adj_factor": "ts_stock_basic",
                      "wind_financial_2014": "ts_stock_basic",
                      'wind_financial_wsd': "ts_stock_basic"}
        basic_name = basic_dict[table_name]
        basic = db[basic_name]
        daily = db[table_name]

        expected_amount = basic.count_documents({"delist_date": {"$eq": None}})
        actual_amount = daily.count_documents({"date": self.nowtime})
        date = self.nowtime
        if expected_amount != actual_amount:
            expected_list = list(basic.find({"delist_date": {"$eq": None}}, {"code": 1, "_id": 0}))
            actual_list = list(daily.find({"date": date}, {"code": 1, "_id": 0}))
            a = list(map(lambda x: x["code"], actual_list))
            e = list(map(lambda x: x["code"], expected_list))

            if expected_amount > actual_amount:
                diff = set(e).difference(set(a))
                print("*数据完整性检验：", table_name, date, "缺失")
                print("缺失code:", diff)
            else:
                print("*数据完整性检验：", table_name, date, "数据完整")
        else:
            print("*数据完整性检验：", table_name, date, "数据完整")

    def check_data_full_not_stock(self, table_name):
        db = self.connect()
        basic_dict = {"wind_option_day": "wind_option_info",
                      "wind_future": "wind_future_info",
                      "wind_index_option_day": "wind_index_option_info",
                      "wind_ftsidx_day": "wind_ftsidx_info",
                      "wind_cbond_day": "wind_cbond_basic",
                      "wind_pfund_day": "wind_pfund_info"}
        basic_name = basic_dict[table_name]
        basic = db[basic_name]
        daily = db[table_name]

        #  可转债的数据完整性需要用status为Normal来查，不能用delist_date
        #  公募基金要用maturity_date来查
        if table_name == "wind_cbond_day":
            expected_amount = basic.count_documents({"status": {"$eq": "Normal"}})
        elif table_name == "wind_pfund_day":
            expected_amount = basic.count_documents({"maturity_date": {"$gt": self.nowtime}})
        else:
            expected_amount = basic.count_documents({"de_listed_date": {"$gt": self.nowtime}})

        actual_amount = daily.count_documents({"date": self.nowtime})
        # print(expected_amount, actual_amount)
        date = self.nowtime
        if expected_amount != actual_amount:
            if table_name == "wind_cbond_day":
                expected_list = list(basic.find({"status": {"$eq": "Normal"}}, {"code": 1, "_id": 0}))
            elif table_name == "wind_pfund_day":
                expected_list = list(basic.find({"maturity_date": {"$gt": self.nowtime}}, {"code": 1, "_id": 0}))
            else:
                expected_list = list(basic.find({"de_listed_date": {"$gt": self.nowtime}}, {"code": 1, "_id": 0}))
            actual_list = list(daily.find({"date": date}, {"code": 1, "_id": 0}))
            # dict形式的把values提出来组成list
            a = list(map(lambda x: x["code"], actual_list))
            e = list(map(lambda x: x["code"], expected_list))

            if expected_amount > actual_amount:
                diff = set(e).difference(set(a))
                # print(len(diff))
                print("*数据完整性检验：", table_name, date, "缺失")
                print("缺失code:", diff)
            else:
                print("*数据完整性检验：", table_name, date, "数据完整")
        else:
            print("*数据完整性检验：", table_name, date, "数据完整")

    # 对于分钟数据，需要用到distinct
    def check_data_full_mint(self, table_name):
        db = self.connect()
        basic_dict = {"jq_future_mint": "wind_future_info",
                      "wind_future_mint": "wind_future_info",
                      "wind_option_mint": "wind_option_info",
                      "wind_index_option_mint": "wind_index_option_info"}
        basic_name = basic_dict[table_name]
        basic = db[basic_name]
        daily = db[table_name]
        actual_list = daily.distinct('code', {'date': self.nowtime})  # return一个list
        actual_amount = len(actual_list)

        # option 只有SZ
        if table_name == "wind_option_mint":
            expected_amount = basic.count_documents({"de_listed_date": {"$gt": self.nowtime}, 'underlying_order_book_id': {'$regex': 'SZ'}})
        else:
            expected_amount = basic.count_documents({"de_listed_date": {"$gt": self.nowtime}})
        date = self.nowtime

        # print(expected_amount, actual_amount)
        if expected_amount != actual_amount:
            if table_name == "wind_option_mint":
                expected_list = list(basic.find({"de_listed_date": {"$gt": self.nowtime}, 'underlying_order_book_id': {'$regex': 'SZ'}}, {"code": 1, "_id": 0}))
            else:
                expected_list = list(basic.find({"de_listed_date": {"$gt": self.nowtime}}, {"code": 1, "_id": 0}))

            # dict形式的把values提出来组成list
            e = list(map(lambda x: x["code"], expected_list))
            if expected_amount > actual_amount:
                diff = set(e).difference(set(actual_list))
                # print(len(diff))
                print("*数据完整性检验：", table_name, date, "缺失")
                print("缺失code:", diff)
            else:
                print("*数据完整性检验：", table_name, date, "数据完整")
        else:
            print("*数据完整性检验：", table_name, date, "数据完整")

    # 针对ETF 场内基金 只有三条的检验
    # wind_index也可以用
    def check_data_full_dict(self, table_name):
        db = self.connect()
        date = self.nowtime
        daily = db[table_name]
        all_dict = self.load_dict()
        basic_list = all_dict[table_name]
        actual_list = list(daily.find({"date": date}, {"code": 1, "_id": 0}))
        a = list(map(lambda x: x["code"], actual_list))
        if Counter(basic_list) == Counter(a):
            print("*数据完整性检验：", table_name, date, "数据完整")
        else:
            diff = set(basic_list).difference(set(a))
            print("*数据完整性检验：", table_name, date, "缺失")
            print("缺失code:", diff)

    def get_report_date(self, now_dt):

        now_dt_md = now_dt[5:10]
        now_dt_year = now_dt[0:4]
        last_year = str(int(now_dt_year) - 1)
        report_date_list = []

        if "01-01" <= now_dt_md < "04-30":
            report_date = last_year + "-09-30"
            report_date_list.append(report_date)
            report_update_date = last_year + "-10-31"

        if "04-30" <= now_dt_md < "08-31":
            report_date = now_dt_year + "-03-31"
            report_date2 = last_year + "-12-31"
            report_date_list.append(report_date2)
            report_date_list.append(report_date)
            report_update_date = now_dt_year + "-04-30"

        if "08-31" < now_dt_md < "10-31":
            report_date = now_dt_year + "-06-30"
            report_date_list.append(report_date)
            report_update_date = now_dt_year + "-08-31"

        if "10-31" < now_dt_md <= "12-31":
            report_date = now_dt_year + "-09-30"
            report_date_list.append(report_date)
            report_update_date = now_dt_year + "-10-31"

        return report_date_list, report_update_date

    def check_data_full_qdata(self, table_name):
        db = self.connect()
        nowdate = self.nowtime
        report_date_list, report_update_date = self.get_report_date(nowdate)
        qtable = db[table_name]
        basic = db["ts_stock_basic"]
        basic_list = basic.distinct('code', {"delist_date": {"$eq": None}})
        for report_date in report_date_list:
            q_list = qtable.distinct('code', {'date': report_date})

            diff1 = set(basic_list).difference(set(q_list))

            if diff1:
                print("*数据完整性检验：", table_name, "截止目前", nowdate, "报告期：%s 有缺失" % report_date)
                res_new = basic.find({"code": {"$in": list(diff1)}, "list_date": {"$gte": report_update_date}}, {"code": 1, "list_date": 1, "_id": 0})
                res_old = basic.find({"code": {"$in": list(diff1)}, "list_date": {"$lt": report_update_date}}, {"code": 1, "list_date": 1, "_id": 0})
                print("缺失code: \n在披露截止日：%s 之后上市的股票：" % report_update_date)
                for i in res_new:
                    print(i)
                print("在披露截止日：%s 之前上市的股票：" % report_update_date)
                for i in res_old:
                    print(i)

            else:
                print("*数据完整性检验：", table_name, "截止目前", nowdate, "报告期：%s 数据完整" % report_date)
            '''
            diff2 = set(q_list).difference(set(basic_list))
            if diff2:
                # 退市了basic的delist date不再是Null，但是依然有年报信息
                res = basic.find({"code": {"$in": list(diff2)}}, {"code": 1, "delist_date": 1, "_id": 0})
                print("*数据完整性检验：", table_name, "报告期：%s 到当前日期：%s 期间有以下股票已退市" % (report_date, nowdate))
                for i in list(res):
                    print(i)
            '''

    def show_null_columns_mint(self, df):
        # print(df['code'].unique())
        for i in df.columns.tolist():
            if i == "code" or i == "datetime":
                continue
            elif not df[i].isnull().any():
                df.drop(i, axis=1, inplace=True)
        df_group = df.sort_values("datetime").groupby("code")
        # print(df_group.agg(lambda x: x.isnull().sum()))
        show_df = df_group.apply(lambda x: x[0:1]).drop(axis=1, columns="code")
        return show_df

    def show_null_columns(self, df):
        df = df.set_index("code")
        columns_name = df.columns.tolist()
        for i in columns_name:
            if i == 'trade_status':
                df.sort_values(by=i, inplace=True, ascending=False)
                continue
            # 如果一整列都不存在Null,删除
            elif not df[i].isnull().any():
                df.drop(i, axis=1, inplace=True)
        return df

    # 3.数据准确性检查
    def check_data_not_null(self, table_name):
        # Null值检查
        df = self.get_df(table_name)
        can_not_null = (self.load_dict())["can_not_null"]
        pdata = df[can_not_null[table_name]]
        nulldata = pdata[pdata.isnull().values == True].drop_duplicates()
        if not nulldata.empty:
            print("*数据Null检验：", table_name, self.nowtime, "存在Null值，具体如下表")
            if 'mint' in table_name:
                nulldf = self.show_null_columns_mint(nulldata)
            else:
                nulldf = self.show_null_columns(nulldata)
            print(nulldf)
        else:
            print("*数据Null检验：", table_name, self.nowtime, "不存在Null值")

    # 对应cbond表 要分cbond和ebond
    def check_data_not_null_cbond(self, table_name):
        df = self.get_df(table_name)
        can_not_null = (self.load_dict())["can_not_null"]
        pdata = df[can_not_null[table_name]]
        nulldata = pdata[pdata.isnull().values == True].drop_duplicates()

        code_list = nulldata["code"].tolist()
        db = self.connect()
        basic = db['wind_cbond_basic']
        res_new = list(basic.find({"code": {"$in": code_list}, "sec_type": "CBond"}, {"code": 1, "_id": 0}))
        cbond_list = list(map(lambda x: x["code"], res_new))
        cbond_nulldata = nulldata[nulldata['code'].isin(cbond_list)]
        ebond = nulldata[~nulldata['code'].isin(cbond_list)].drop(['pct_chg', '_end_update', 'amount_ma12', 'avg_price', 'rate_bond', 'rate_issuer'], axis=1)
        ebond_nulldata = ebond[ebond.isnull().values == True].drop_duplicates()

        if not cbond_nulldata.empty:
            print("*数据Null检验：", table_name, self.nowtime, "Cbond 存在Null值，具体如下表")
            nulldf = self.show_null_columns(cbond_nulldata)
            print(nulldf)
        if not ebond_nulldata.empty:
            print("*数据Null检验：", table_name, self.nowtime, "Ebond 存在Null值，具体如下表")
            nulldf = self.show_null_columns(ebond_nulldata)
            print(nulldf)
        if ebond_nulldata.empty and cbond_nulldata.empty:
            print("*数据Null检验：", table_name, self.nowtime, "不存在Null值")

    # 对应future表 要把55 66 77 88 99挑出来
    def check_data_not_null_future(self, table_name):
        df = self.get_df(table_name)
        can_not_null = (self.load_dict())["can_not_null"]
        # pdata = df[can_not_null[table_name]]
        c_list = can_not_null[table_name]
        pdata = df.loc[:, c_list]
        # nulldata = pdata[pdata.isnull().values == True].drop_duplicates()
        nulldata = pdata.loc[pdata.isnull().values == True].drop_duplicates()

        code_list = nulldata["code"].tolist()
        ab_code_list = list(filter(lambda x: not re.match('[A-Za-z]+(55|66|77|88|99)$', x), code_list))
        aa_code_list = list(set(code_list).difference(set(ab_code_list)))
        nulldata_copy = nulldata.copy()

        ab_nulldata = nulldata_copy.loc[nulldata_copy['code'].isin(ab_code_list)]
        # ab_nulldata = nulldata_copy[nulldata_copy['code'].isin(ab_code_list)]
        aaa = nulldata_copy.loc[nulldata_copy['code'].isin(aa_code_list)]
        aa = aaa.drop(['limit_down', 'limit_up'], axis=1)
        # aa = nulldata_copy[~nulldata_copy['code'].isin(ab_code_list)].drop(['limit_down', 'limit_up'], axis=1)
        aa_nulldata = aa.loc[aa.isnull().values == True].drop_duplicates()

        if ab_nulldata.empty and aa_nulldata.empty:
            print("*数据Null检验：", table_name, self.nowtime, "不存在Null值")
        else:
            print("*数据Null检验：", table_name, self.nowtime, "存在Null值，具体如下表")
            if not ab_nulldata.empty:
                if 'mint' in table_name:
                    nulldf = self.show_null_columns_mint(ab_nulldata)
                else:
                    nulldf = self.show_null_columns(ab_nulldata)
                print(nulldf)
            if not aa_nulldata.empty:
                if 'mint' in table_name:
                    nulldf = self.show_null_columns_mint(aa_nulldata)
                else:
                    nulldf = self.show_null_columns(aa_nulldata)
                print(nulldf)

    def check_data_not_negative(self, table_name):
        # 非负值检查
        df = self.get_df(table_name)
        can_not_negative = (self.load_dict())["can_not_negative"]
        not_negative_data = df[can_not_negative[table_name]]

        c_columns = not_negative_data.loc[:, (not_negative_data < 0).any()].columns.tolist()
        c_columns.insert(0, 'code')
        if 'mint' in table_name:
            c_columns.insert(1, 'datetime')

        neg_res = not_negative_data[(not_negative_data < 0).any(axis=1)]
        if neg_res.empty:
            print("*数据非负检验：", table_name, self.nowtime, "不存在异常负值数据")
        else:
            print("*数据非负检验：", table_name, self.nowtime, "存在异常负值数据，具体如下表")
            idx = neg_res.index.tolist()
            neg_data = df.loc[idx, c_columns]
            print(neg_data)

    def check_data_positive(self, table_name):
        # 正数检查
        df = self.get_df(table_name)
        must_positive = (self.load_dict())["must_positive"]
        pos_data = df[must_positive[table_name]]

        c_columns = pos_data.loc[:, (pos_data <= 0).any()].columns.tolist()
        c_columns.insert(0, 'code')
        if 'mint' in table_name:
            c_columns.insert(1, 'datetime')
        not_positive_res = pos_data[(pos_data <= 0).any(axis=1)]

        if not_positive_res.empty:
            print("*数据正值检验：", table_name, self.nowtime, "不存在异常非正值数据")
        else:
            print("*数据正值检验：", table_name, self.nowtime, "存在异常非正值数据，具体如下表")
            idx = not_positive_res.index.tolist()
            not_positive_data = df.loc[idx, c_columns]
            print(not_positive_data)

    def check_north_capital(self, table_name):
        df = self.get_df(table_name)
        zcsdata = df[["sz_net_purchases", "sh_net_purchases"]]
        db2 = self.connect_news_class()
        newstable = db2["Foreign_Capital_Plus"]
        newsdata = pd.DataFrame(list(newstable.find({"datetime": self.nowtime}, {"_id": 0, "total_chg": 1})))
        if newsdata.empty:
            print("*Error Foreign_Capital_Plus %s 未更新" % self.nowtime)
        else:
            zcsdata["net_purchases"] = zcsdata.sum(axis=1)
            zcsdata["Foreign_Capital_Plus"] = newsdata["total_chg"] / 10
            zcsdata["diff"] = zcsdata["net_purchases"] - zcsdata["Foreign_Capital_Plus"]
            zcsdata[table_name] = zcsdata["net_purchases"]
            print("*北上资金计算检查：%s & Foreign_Capital_Plus %s 结果如下" % (table_name, self.nowtime))
            print(zcsdata[[table_name, "Foreign_Capital_Plus", "diff"]])

    def get_df(self, table_name):
        db = self.connect()
        table = db[table_name]
        data = pd.DataFrame(list(table.find({"date": self.nowtime}, {"_id": 0})))
        return data

    def loop_table(self):
        table_list = self.get_table_list("check_update_time")

        for table_name in table_list:
            print("\n")
            print("***********TABLE: %s************" % table_name)
            status = self.check_update_time(table_name)
            if status:  # or not status:
                # 先对不同各类表做完整性检查
                # ETF fund单独拿出来因为只有三只
                full_fun_list = ["check_data_full_stock", "check_data_full_not_stock", "check_data_full_dict", "check_data_full_mint",
                                 "check_data_full_qdata", "check_data_not_null_future"]
                for full_fun_name in full_fun_list:
                    table_list2 = self.get_table_list(full_fun_name)
                    if table_name not in table_list2:
                        continue
                    else:
                        getattr(self, full_fun_name)(table_name)

                # func_name = ["Null", "非负", "正值"]
                # fun_dict = dict(zip(fun_list, func_name))

                fun_list = ["check_data_not_null", 'check_data_not_null_cbond', "check_data_not_negative", "check_data_positive", "check_north_capital"]
                for fun_name in fun_list:
                    table_list3 = self.get_table_list(fun_name)
                    if table_name not in table_list3:
                        continue
                    else:
                        getattr(self, fun_name)(table_name)
            else:
                print("*Error: 数据暂未更新，无法进行其他检查")


