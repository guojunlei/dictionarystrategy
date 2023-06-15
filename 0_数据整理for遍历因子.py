"""
为golang版策略因子遍历做准备工作
导入数据:整理数据all_stock_data
分割周期: "W"周,"M"月,"Y"年 None 不分割
warmup周期: 52 周为单位
factor_list: 所有因子columns名称
默认保留列:trade_date  is_trade next_is_trade next_open_up next_open_return next_every_day_return
"""

import pandas as pd
import numpy as np

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 5000)

# 需要修改设置
# --- 保留列,名称对应上,如果名称和默认一样,填None
reserve_columns_dict = {
    "trade_date": "日期",  # 交易日期
    "is_trade": None,  # 是否交易
    "next_is_trade": None,  # 下日是否交易
    "next_open_up": None,  # 下日是否涨停
    "next_open_return": None,  # 下日开盘涨跌幅
    "next_every_day_return": None,  # 下周期每日涨跌幅
}

factor_list = ['new_on_balance_volume', 'fund_size_neutral', 'eri_high_5', 'eri_high_10',
               'eri_high_20', 'eri_low_5', 'eri_low_10', 'eri_low_20', 'emv_5', 'emv_10',
               'emv_20', 'amplitude_5', 'amplitude_10', 'amplitude_20', 'turnover_rate_5',
               'turnover_rate_10', 'turnover_rate_20', 'k_indicator_5', 'k_indicator_10',
               'k_indicator_20', 'd_indicator_5', 'd_indicator_10', 'd_indicator_20',
               'j_indicator_5', 'j_indicator_10', 'j_indicator_20', 'investment_income_coverage',
               'interest_income_coverage', 'profit_margin', 'peg', 'eps',
               'expense_coverage_ratio', 'roa', 'debt_to_equity_ratio', 'roe',
               'pb_ratio', 'pe_ratio', '成交额_std_5', 'Stochastic_K_20', 'Volume_Ratio_20',
               'MeanAmplitude_5', '成交额_mean_5', 'On_balance_value', 'Popularity_Index_20',
               'K线长度_max_20', '成交额_std_10', 'bais_5', 'bais_10', 'bais_20', '涨跌幅_10',
               '涨跌幅_20', '量价相关系数_10', 'alpha150', '涨跌幅_std_5', '涨跌幅_std_10',
               '涨跌幅_std_20', '涨跌幅_mean_10', '涨跌幅_mean_20', 'close']


# 检查是否有需要重命名的默认列
def check_reserve_name(d: dict) -> dict:
    new_dict = {}
    new_list = []
    for k, v in d.items():
        new_list.append(k)
        if v is not None:
            new_dict[v] = k
    return new_dict, new_list


# 计算下周期收益率
def calculate_return(l: list) -> float:
    arr = l['next_every_day_return']
    arr[0] = l['next_open_return']
    arr = np.array(arr)
    r = np.prod(arr + 1) - 1
    return r


if __name__ == "__main__":
    # 导入数据
    df: pd.DataFrame = pd.read_pickle("./data/all_stock_data.pkl")

    # 整理数据
    # --- 默认列,检查是否有需要改的默认列名
    rename_dict, reserve_list = check_reserve_name(reserve_columns_dict)

    # --- 重命名,删除多余列
    df.rename(columns=rename_dict, inplace=True)
    df = df[reserve_list + factor_list]
    df.dropna(subset=['next_every_day_return'], inplace=True)

    # 计算下周收益率
    df['next_return'] = df[['next_open_return', 'next_every_day_return']].apply(calculate_return, axis=1)
    del df['next_every_day_return']
    del df['next_open_return']
    # 排序
    df.sort_values(['trade_date'], inplace=True)
    df.index = range(df.shape[0])
    df.to_csv("output/all_stock_data.csv", index=0)
