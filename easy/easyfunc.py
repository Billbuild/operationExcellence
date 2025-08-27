# 应用内模块
import easy.constants as const
import easy.easyclass as ec

# 内置模块
import os
import re
import datetime as dt

# Django模块

# 第三方模块
import numpy as np
import pandas as pd


def get_obj_attr(obj):
    """
    目的：
    获取对象的属性
    :param obj:
    :return: 对象的属性列表
    """
    return [name for name in obj.__dict__.keys() if not name.startswith('_')]


def get_obj_value(obj):
    return [value for key, value in obj.__dict__.items() if not key.startswith('_')]


def get_obj_dict(obj):
    return {key: value for key, value in obj.__dict__ if not key.startswith('_')}


def get_variable_name(dic, value):
    """
    目的: 如果知道了字典中 value 的值，那么返回其中的一个 key
    :param dic: 字典
    :param value: 字典中的 value
    :return: 字典中的 key
    """

    li = []
    for k, v in dic.items():
        if dic[k] is value:
            li.append(k)

    return li


def post_to_dic(request):
    dic = {key: value[0] for key, value in dict(request.POST).items()}
    return dic


def remove_duplicates(nested_list):
    """
    :param nested_list: 二维列表, 元素是一维元祖或者一维列表
    :return: 去除重复一维值之后的二维列表, 元素是一维元祖
    """
    return list(set(tuple(x) for x in nested_list))


# ***** 文件函数 *****


def file_exl_to_csv(exl_path, csv_path):
    """
    将 Excel 文件转换为 csv 文件
    :param exl_path:
    :param csv_path:
    :return:
    """
    df = pd.read_excel(exl_path)
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')

    return None


def dir_exl_to_csv(exl_dir, csv_dir):
    """
    将目录中的 Excel 文件转化为 csv 文件
    :param exl_dir:
    :param csv_dir:
    :return:
    """
    for exl_file_name in os.listdir(exl_dir):
        if '.xlsx' in exl_file_name:
            exl_file_path = os.path.join(exl_dir, exl_file_name)

            csv_file_name = exl_file_name.split('.')[0] + '.csv'
            csv_file_path = os.path.join(csv_dir, csv_file_name)

            file_exl_to_csv(exl_file_path, csv_file_path)

    return None


def combine_csv_files(source_dir, target_path):
    """
    目的：
    将文件夹中的 .csv 文件合并为一个 .csv 文件，需要文件夹中的 .csv 文件有相同的数据结构
    :param source_dir: 字符串
    :param target_path: 字符串
    :return: None
    """

    df_combine = pd.DataFrame()

    for file_name in os.listdir(source_dir):
        if '.csv' in file_name:
            source_file_path = os.path.join(source_dir, file_name)
            df = pd.read_csv(source_file_path)
            df_combine = pd.concat([df_combine, df])

    df_combine.to_csv(target_path, index=False, encoding='utf-8-sig')


def excel_is_open(filename):
    """
    目的: 检查 excel 文件是否打开
    逻辑:
        例如: 如果 test.xlsx, 文件夹中会有隐藏文件 ~$test.xlsx 文件, 通过检查 ~$test.xlsx 是否存在, 判断 test.xlsx 是否打开
    :param filename: 包含路径的 excel 文件名
    :return:
    """
    excel_path = os.path.dirname(filename)
    excel_name = os.path.split(filename)[-1]
    hide_excel_name = excel_path + r'\~$' + excel_name
    print(hide_excel_name)
    if os.path.exists(hide_excel_name):
        return True
    else:
        return False


def find_file_by_suffix(directory, file_suffix):
    """
    目的： 找到指定目录内 所有规定后缀的 文件的 路径
    :param directory: 指定的目录
    :param file_suffix: 需要寻找的文件的后缀名
    :return: 列表，元素包含指定目录内所有的指定后缀名的路径，包含目录和文件名
    """
    # file_name_li 列表中的元素是文件的名称
    files_name_li = [file_name for file_name in os.listdir(directory) if file_name.endswith(file_suffix)]
    # file_path_li 列表中的元素是包含了目录和文件的名称，是一个文件完整的路径
    files_path_li = [os.path.join(directory, file_name) for file_name in files_name_li]

    return files_path_li


def sql_to_json(tb_name, json_path, data_status=0):
    """
    data_status = 0
    [
        {"id": "c001", "name": "天天向上", "type": "company", "parent": null},
        {"id": "f001", "name": "杭州工厂", "type": "factory", "parent": "c001"},
    ]
    data_status = 1
    {
        "c001", {"id": "c001", "name": "天天向上", "type": "company", "parent": null},
        "f001", {"id": "f001", "name": "杭州工厂", "type": "factory", "parent": "c001"},
    }
    :param tb_name:
    :param json_path:
    :param data_status:
    :return:
    """
    from sqlalchemy import create_engine, text
    import easy.json_handle as jh

    engine = create_engine(const.ENGINE_OPEX)
    conn = engine.connect()
    query = text(f"select * from {tb_name}")
    result = conn.execute(query)

    rows = result.all()
    field_names = list(result.keys())

    data_to_json = []
    for row in rows:
        data_to_json.append(dict(zip(field_names, row)))

    if data_status == 0:
        jh.export_dict_to_json(data_to_json, json_path)

    if data_status == 1:
        id_index_data = {item["id"]: item for item in data_to_json}
        jh.export_dict_to_json(id_index_data, json_path)


# ***** 字符 字符串 *****


def mandarin_first_letter(ch):
    """
    目的: 获取中文字符拼音的首字母
    :param ch: 汉字字符
    :return: 一个小写的英文字母
    """
    ch = ch.encode('gbk')
    # 一个汉字一个列表, 这个列表有两个元素
    asc = ch[0] * 256 + ch[1] - 65536
    if asc >= -20319 and asc <= -20284:
        return 'a'
    if asc >= -20283 and asc <= -19776:
        return 'b'
    if asc >= -19775 and asc <= -19219:
        return 'c'
    if asc >= -19218 and asc <= -18711:
        return 'd'
    if asc >= -18710 and asc <= -18527:
        return 'e'
    if asc >= -18526 and asc <= -18240:
        return 'f'
    if asc >= -18239 and asc <= -17923:
        return 'g'
    if asc >= -17922 and asc <= -17418:
        return 'h'
    if asc >= -17417 and asc <= -16475:
        return 'j'
    if asc >= -16474 and asc <= -16213:
        return 'k'
    if asc >= -16212 and asc <= -15641:
        return 'l'
    if asc >= -15640 and asc <= -15166:
        return 'm'
    if asc >= -15165 and asc <= -14923:
        return 'n'
    if asc >= -14922 and asc <= -14915:
        return 'o'
    if asc >= -14914 and asc <= -14631:
        return 'p'
    if asc >= -14630 and asc <= -14150:
        return 'q'
    if asc >= -14149 and asc <= -14091:
        return 'r'
    if asc >= -14090 and asc <= -13119:
        return 's'
    if asc >= -13118 and asc <= -12839:
        return 't'
    if asc >= -12838 and asc <= -12557:
        return 'w'
    if asc >= -12556 and asc <= -11848:
        return 'x'
    if asc >= -11847 and asc <= -11056:
        return 'y'
    if asc >= -11055 and asc <= -10247:
        return 'z'


def initial_mandarin_string(string):
    """
    目的：、
    获取中文字符串的英文小写首字母
    :param string: 中文字符串
    :return: 英文小写字符串
    """
    li = []
    for ch in string:
        li.append(mandarin_first_letter(ch))
    initial_string = ''.join(li)

    return initial_string


def camel_to_underline(camel_str):
    """
    目的: 将驼峰字转换成两个单词用下划线连接
    :param camel_str: 首字母大写的驼峰字
    :return:
    """
    index = []
    str_li = []
    for i in range(len(camel_str)):
        # 97 65 之间的字符是 大写字母
        if 97 > ord(camel_str[i]) >= 65:
            index.append(i)
    length = len(index)
    for i in range(length):
        # 当 i 不是 str_li 数组的最后一个元素时
        if i < length - 1:
            # 前一个大写字母的位置和下一个大写字母的位置作为驼峰字的切片
            str_li.append(camel_str[index[i]: index[i+1]].lower())
        # 当 i 是 str_li 数组的最后一个元素时
        else:
            str_li.append(camel_str[index[i]:].lower())
    string = '_'.join(str_li)
    return string


def count_chinese_characters(string):
    """
    目的：
    计算字符串中包含中文字符的个数
    :param string: 字符串类型
    :return: int
    """
    return len(re.findall(r'[\u4e00-\u9fff]', string))


def financial_string_to_number(financial_str):
    """
    用途：将财务数字型的字符串转换成数值
    :param financial_str:
    :return:
    """
    if ',' in financial_str:
        result = str.replace(financial_str, ',', '')
        if '.' in result:
            return float(result)
        else:
            return int(result)
    else:
        if '.' in financial_str:
            return float(financial_str)
        else:
            return int(financial_str)


def list_convert_to_letter(li):
    """
    目的：依据列表 li 的元素位置依次生成字符 a, b, c, ..., 并将这些字母组成列表
    注意：li 长度不要超过 26，否则会产生不可视字符
    :param li:
    :return:
    """
    if len(li) <= 26:
        return [chr(97 + i) for i in range(len(li))]
    else:
        return None


# ***** 时间函数 *****


def mtd_first_day(date):
    """
    用途：
    找到日期所在月份的第一天
    :param date: dt.date 或者 dt.datetime
    :return: dt.date 或者 dt.datetime
    """
    if isinstance(date, dt.date) | isinstance(date, dt.datetime):
        # return date.replace(day=1).strftime('%Y-%m-%d')
        return date.replace(day=1)
    # if isinstance(date, str):
    #     if is_date_format_string(date):
    #         year = date.split('-')[0]
    #         month = date.split('-')[1]
    #         return '-'.join([year, month, '01'])
    #     else:
    #         print('the date string does not comply for the format string')


def last_day_in_month(cur_date):
    """
    目的：获取指定日期所在月份的最后一天
    :param cur_date: dt.date 类型
    :return: dt.date 类型
    """
    import calendar

    month = cur_date.month
    year = cur_date.year

    _, last_day = calendar.monthrange(year, month)
    return dt.date(year, month, last_day)


def last_month(cur_date):

    month = cur_date.month
    year = cur_date.year

    if month in range(2, 13):
        month = month - 1
    if month == 1:
        month = 12
        year = year - 1

    return year, month


def last_month_as_period(cur_date):

    year, month = last_month(cur_date)
    first_day = dt.date(year, month, 1)
    last_day = last_day_in_month(first_day)

    return first_day, last_day



def ytd_first_day(date):
    """
    用途：
    找到日期所在年份的第一天
    :param date: dt.date 或者 dt.datetime
    :return: dt.date 或者 dt.datetime
    """
    if isinstance(date, dt.date) | isinstance(date, dt.datetime):
        # return date.replace(day=1).replace(month=1).strftime('%Y-%m-%d')
        return date.replace(day=1).replace(month=1)
    # if isinstance(date, str):
    #     if is_date_format_string(date):
    #         year = date.split('-')[0]
    #         return '-'.join([year, '01', '01'])
    #     else:
    #         print('the date string does not comply for the format string')


def past_30_days(target_date):
    """
    目的：返回一个日期，这个日期距 target_date(含）有 30 天
    :param target_date: dt.datetime.date
    :return:
    """
    end_date = target_date
    start_date = end_date - dt.timedelta(days=29)
    return start_date, end_date


def last_year_month(date):
    month = date.month
    year = date.year
    if month == 1:
        month = 12
        year = year - 1
    else:
        month = month - 1
        year = year
    return year, month


def is_leap_year(year):
    is_leap = year % 4 == 0 and year % 100 != 0 or year % 400 == 0
    return is_leap


def now_to_str(now):
    """
    目的: 将 now() 转换成到秒的数字字符串. 例如 2012-12-24 11:24:33.34567 转换成 20121224112433
    :param now: dt.datetime.now()
    :return:
    """
    now_second_list = str(now).split(' ')
    date = ''.join(now_second_list[0].split('-'))
    second = ''.join(now_second_list[1].split(':'))
    return (date + second).split('.')[0]


def time_gap(time_start, time_end):
    """
    用途：计算两个时间之间间隔多少小时
    :param time_start: dt.datetime()
    :param time_end: dt.datetime()
    :return:
    """
    if time_end >= time_start:
        return (time_end - time_start).total_seconds() / 3600
    else:
        return (time_end - time_start + dt.timedelta(days=1)).total_seconds() / 3600


def is_time_format_string(time_format_string):
    """
    用途：判断时间字符串是否合格
    7:30 合格， 7:66 不合格
    7:3 合格
    07:30 合格
    07:3 合格
    :param time_format_string:
    :return:
    """
    pattern = r"^(\d{1,2}):(\d{1,2})$"
    match = re.search(pattern, time_format_string)
    if match:
        hour_string, minute_string = match.group(1), match.group(2)
        if int(hour_string) < 24 and int(minute_string) < 60:
            return True
        else:
            return False
    else:
        return False


def is_date_format_string(date_format_string):
    """
    用途：判断时间字符串是否合格
    2024-1-15 合格，2024-1-32 不合格
    2024-01-15 合格
    2024-01-5 合格
    2024-1-5 合格
    :param date_format_string:
    :return:
    """
    month_days = {
        1: 31, 2: 29, 3: 31, 4: 30, 5: 31, 6: 30,
        7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31
    }

    pattern = r"^\d{4}-(\d{1,2})-(\d{1,2})$"
    # 如果符合 pattern， 返回 <re.Match object; span=(0, 9), match='2033-12-9'>
    # 如果不符合，返回 None
    match = re.search(pattern, date_format_string)

    if match:
        month, day = int(match.group(1)), int(match.group(2))
        if 1 <= month <= 12 and 1 <= day <= month_days[month]:
            return True
        else:
            return False
    else:
        return False


def transform_date_str(date_str):
    """
    用途：
    将 yyyy-mm-dd 转换成 yyyymmdd
    将 yyyymmdd 转换成 yyyy-mm-dd
    :param date_str:
    :return:
    """

    # *****-> 不能检测到闰月的最后一天 <-*****
    # 日期格式 yyyy-mm-dd
    hyphen_date = r'^\d{4}-\d{2}-\d{2}$'
    # 日期格式 yyyymmdd
    number_date = r'^\d{4}\d{2}\d{2}$'
    # yyyy-mm-dd 转换成 yyyymmdd
    if re.match(hyphen_date, date_str):
        month = int(date_str[5:7])
        day = int(date_str[-2:])
        if (month in [1, 3, 5, 7, 8, 10, 12] and day <= 31) or (month == 2 and day <= 29) or (month in [4, 6, 9, 11] and day <= 30):
            return ''.join(date_str.split('-'))
        else:
            print(f"{date_str}不是合规的日期。")
            return False
    # yyyymmdd 转换成 yyyy-mm-dd
    elif re.match(number_date, date_str):
        month = int(date_str[4:6])
        day = int(date_str[-2:])
        if (month in [1, 3, 5, 7, 8, 10, 12] and day <= 31) or (month == 2 and day <= 29) or (month in [4, 6, 9, 11] and day <= 30):
            return f"{date_str[0:4]}-{date_str[4:6]}-{date_str[6:]}"
        else:
            print(f"{date_str}不是合规的日期。")
            return False
    # 不合规的 date_str
    else:
        print(f"{date_str}不是合规的日期。")
        return False


def date_list_from_range(start_date, end_date):
    """
    目的：获取一个日期字符串列表，这个列表包含 start_date 和 end_date 之间的所有日期
    :param start_date: yyyy-mm-dd
    :param end_date: yyyy-mm-dd
    :return: [yyyy-mm-dd, ]
    """

    s_list = start_date.split('-')
    s_year = int(s_list[0])
    s_monty = int(s_list[1])
    s_day = int(s_list[2])
    s_date = dt.datetime(s_year, s_monty, s_day)

    e_list = end_date.split('-')
    e_year = int(e_list[0])
    e_monty = int(e_list[1])
    e_day = int(e_list[2])
    e_date = dt.datetime(e_year, e_monty, e_day)

    date_list = [
        (s_date + dt.timedelta(days=i)).strftime('%Y-%m-%d')
        for i in range((e_date - s_date).days + 1)
    ]
    return date_list


def date_list_from_month(year_month):
    """
    目的：获取一个日期列表，列表包含指定月份的每一天
    :param year_month: 字符串 yyyy-mm
    :return: [yyyy-mm-dd, ]
    """
    # 解析 yyyy-mm 格式的字符串
    year, month = map(int, year_month.split('-'))

    # 获取该月份的第一天和最后一天
    first_day = dt.datetime(year, month, 1)
    if month == 12:  # 如果月份是 12 月，下个月是下一年的 1 月
        next_month = dt.datetime(year + 1, 1, 1)
    else:
        next_month = dt.datetime(year, month + 1, 1)
    last_day = next_month - dt.timedelta(days=1)  # 该月份的最后一天

    # 生成该月份的所有日期
    date_list = [
        (first_day + dt.timedelta(days=i)).strftime('%Y-%m-%d')
        for i in range((last_day - first_day).days + 1)
    ]

    return date_list


def date_list_from_datetype(start_date, end_date):
    """

    :param start_date: dt.datetime.date
    :param end_date: dt.datetime.date
    :return: date_list, 元素是 dt.datetime.date
    """


# ***** 数字函数 *****

def is_achieved(high_better, actual, target):
    """
    目的：判断 actual 的值是否达成了 target，通常用于实际发生值和目标值的比较
    :param high_better: boolean
    :param actual: float
    :param target: float
    :return: boolean
    """
    if high_better:
        if actual >= target:
            return True
        else:
            return False
    else:
        if actual <= target:
            return True
        else:
            return False


# 被面元函数 pd.cut 取代了
# def number_range(x, number_list):
#     """
#     用途：
#     将 x 和 number_list 中的值依次进行比较，确定 x 所在的区间。
#     这个区间由 number_list 的数值构成
#     :param x: int | float
#     :param number_list: 包含数值的列表，其中的每个数值按顺序递增
#     :return:
#     """
#     for i in range(len(number_list)-1):
#         if x < number_list[i + 1]:
#             return f"[{number_list[i]}-{number_list[i+1]}]"
#     else:
#         print(f"{x} 超过了区间最大值")
#         return None

# 被面源函数 pd.cut 取代了
# def custom_sort_range(number_list):
#     """
#     用途：
#     由 number_list 构成的区间，当中的数值已经是字符串，不能够按照数值的大小进行排序
#     需要指定这些的排序顺序
#     比如 ['[0-50]', '[50-100]', '[100-150]', '[150-300]', '[300-450]', '[450-1000]']
#     :param number_list: 包含数值的列表，其中的每个数值按顺序递增
#     :return:
#     """
#     sort_list = []
#     for i in range(len(number_list)-1):
#         gap_range = f"[{number_list[i]}-{number_list[i+1]}]"
#         sort_list.append(gap_range)
#     return sort_list


if __name__ == '__main__':

    date = dt.date(2025, 11, 3)
    start_date, end_date = last_month_as_period(date)
    print(start_date, end_date)
    # print(date.month)

