"""
制作因子文件给golang使用
          名称                         方向
["factor1","factor2" ...]       [true,false...]
"""

import pandas as pd
import itertools
from tqdm import tqdm
import warnings

warnings.filterwarnings('ignore')

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 5000)

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


# 因子组合
def matching_factor_strategy(factor_list, num):
    code_list = []
    for code in factor_list:
        code_list.append(code)
    combination = list(itertools.combinations(code_list, num))
    return combination


if __name__ == "__main__":
    factor_num = 5
    column_list = ["因子", "方向"]

    if factor_num == 1:
        df_list = []
        for f in factor_list:
            g = pd.DataFrame([[str([f]), str(["false"])]], columns=column_list)
            e = pd.DataFrame([[str([f]), str(["true"])]], columns=column_list)
            df_list.append(g)
            df_list.append(e)
        df = pd.concat(df_list, axis=0)
        df.index = range(df.shape[0])
        df.to_csv(f"factorFile/factor_{factor_num}.csv", index=0)
    if factor_num != 1:
        factor_list = matching_factor_strategy(factor_list, factor_num)

        one_factor = pd.read_csv("result/combination_1.csv", )

        df_list = []
        for f in tqdm(factor_list):
            direct = []
            for i in f:
                d = one_factor[one_factor['因子'] == i]
                d.sort_values(['净值'], inplace=True)
                direction = d["方向"].iloc[-1]
                direct.append(str(direction).lower())
            f = pd.DataFrame([["[" + str(f)[1:-1] + "]", str(direct)]], columns=column_list)
            df_list.append(f)
        df = pd.concat(df_list, axis=0)
        df.index = range(df.shape[0])
        df.to_csv(f"factorFile/factor_{factor_num}.csv", index=0)
