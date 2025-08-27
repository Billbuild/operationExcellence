import easy.json_handle as jh

from pathlib import Path
from enum import Enum

# *** 2025-6-23 ***
CONFIGS_DIR = Path(__file__).resolve().parent.parent / 'configs'
CONFIG_ORGANIZATION = CONFIGS_DIR / 'organization_config.json'
CONFIG_METRIC = CONFIGS_DIR / 'metric_config.json'

DIR_DIMENSIONS = r'D:\operationExcellence\measures\dimensions'
DIR_TRANSACTIONS = r'D:\operationExcellence\measures\transactions'
DIR_ANALYSIS = r'D:\operationExcellence\measures\analysis'
DIR_REPORTS = r'D:\operationExcellence\measures\reports'


class METRICS(Enum):
    OWE = 'owe'
    SAM_COST = 'sam_cost'
    # WIP = 'wip'
    LEAD_TIME = 'lead_time'
    BTS = 'bts'
    HOT = 'hot'
    RFT = 'rft'
    FINAL_REWORK = 'final_rework'

    @classmethod
    def metric_en_dic(cls):
        return {
            cls.OWE.value: 'OWE',
            cls.SAM_COST.value: 'Sam Cost',
            # cls.WIP.value: 'WIP',
            cls.LEAD_TIME.value: 'Lead Time',
            cls.BTS.value: 'BTS',
            cls.HOT.value: 'HOT',
            cls.RFT.value: 'RFT',
            cls.FINAL_REWORK.value: 'Final Rework'
        }

    @classmethod
    def metric_cn_dic(cls):
        return {
            cls.OWE.value: '全员生产效率',
            cls.SAM_COST.value: '工时分钟成本',
            # cls.WIP.value: '在制品',
            cls.LEAD_TIME.value: '前置时间',
            cls.BTS.value: '计划达成率',
            cls.HOT.value: '准交率',
            cls.RFT.value: '一次合格率',
            cls.FINAL_REWORK.value: '终检回修率'

        }

    @classmethod
    def high_better_dic(cls):
        return {
            cls.OWE.value: True,
            cls.SAM_COST.value: False,
            # cls.WIP.value: False,
            cls.LEAD_TIME.value: False,
            cls.BTS.value: True,
            cls.HOT.value: True,
            cls.RFT.value: True,
            cls.FINAL_REWORK.value: False
        }

    @classmethod
    def metric_unit_dic(cls):
        return {
            cls.OWE.value: '%',
            cls.SAM_COST.value: '元',
            # cls.WIP.value: '天',
            cls.LEAD_TIME.value: '天',
            cls.BTS.value: '%',
            cls.HOT.value: '%',
            cls.RFT.value: '%',
            cls.FINAL_REWORK.value: '%'
        }

    @classmethod
    def get_member_from_value(cls, value):
        """
        目的：通过 value 获取枚举成员
        """
        for member in cls:
            if member.value == value:
                return member
        raise ValueError(f"{value} 不是有效的 {cls.__name__} 值")

    @classmethod
    def metric_name_list(cls):
        """
        :return: 英文的运营指标列表
        """
        return [member.value for member in cls]

    @classmethod
    def metric_en_list(cls):
        li = cls.metric_name_list()
        li_en = []
        for item in li:
            li_en.append(cls.metric_en_dic()[item])
        return li_en


# *** 2025-6-23 ***

# json 配置文件

CONFIG_TABLE = jh.load_json_as_dic(CONFIGS_DIR / 'table_config.json')
CONFIG_CSV = jh.load_json_as_dic(CONFIGS_DIR / 'csv_config.json')
CONFIG_FACTORY = jh.load_json_as_dic(CONFIGS_DIR / 'factory_config.json')


# 日志文件
LOG_DIR = Path(__file__).resolve().parent.parent / 'logs'
LOG_MEASURES_DIC = jh.load_json_as_dic(LOG_DIR / 'log_db_measures.json')

# 数据库
# 引擎
ENGINE_OPEX = 'mysql+pymysql://Bill:1111@localhost:3306/opex'

# 文件
MEASURE_DIR = r'D:\operationExcellence\measures'
# 日志文件
# 上传文件到数据库，需要写到日志文件，日志文件的地址
LOG_DB_MEASURES_PATH = Path(__file__).resolve().parent.parent / 'logs/log_db_measures.json'

# 工厂
FACTORIES = ['01衢州', '02龙游', '03越南', '04孟加拉']


# 表
class Tables(Enum):
    # 报工表 源表和合规表
    OUTPUT = 'output'
    # 考勤表 源表和合规表
    ATTENDANCE = 'attendance'
    # 单位表 中间表
    ORGANS = 'organs'
    # 员工表 中间表
    EMPLOYEES = 'employees'
    # 单位 owe 分析表
    ORGAN_OWE = 'organ_owe'
    # 款式 owe 分析表
    STYLE_OWE = 'style_owe'
    # 单位 owe 报表
    ORGAN_OWE_REPORT = 'organ_owe_report'


# metric
class Metrics(Enum):
    OWE_EN = 'OWE'
    SAM_COST_EN = 'Sam Cost'
    WIP_EN = 'WIP'
    LEAD_TIME_EN = 'Lead Time'
    BTS_EN = 'BTS'
    HOT_EN = 'HOT'
    RFT_EN = 'RFT'
    FR_EN = 'Final Rework'
    OWE_CN = '全员生产效率'
    SAM_COST_CN = '工时分钟成本'
    WIP_CN = '在制品'
    LEAD_TIME_CN = '前置时间'
    BTS_CN = '计划达成率'
    HOT_CN = '准交率'
    RFT_CN = '一次合格率'
    FR_CN = '终检回修率'
    OWE_WORD = 'owe'
    SAM_COST_WORD = 'sam_cost'
    WIP_WORD = 'wip'
    LEAD_TIME_WORD = 'lead_time'
    BTS_WORD = 'bts'
    HOT_WORD = 'hot'
    RFT_WORD = 'rft'
    FR_WORD = 'final_rework'

    @classmethod
    def get_member_from_value(cls, value):
        """
        目的：通过 value 获取枚举成员
        """
        for member in cls:
            if member.value == value:
                return member
        raise ValueError(f"{value} 不是有效的 {cls.__name__} 值")

    @classmethod
    def get_member_en(cls, value):
        """
        目的：通常是为了得到 en member 用于 OrganMetric 的参数
        :param value: WORD 的 value
        :return:
        """
        en_name = cls.get_member_from_value(value).name
        if 'EN' in en_name:
            return cls[en_name]
        else:
            parts = en_name.rsplit('_', 1)
            en_name = parts[0] + '_EN'
            return cls[en_name]

    @classmethod
    def metric_list(cls):
        """

        :return: ['owe', 'sam_cost', 'wip', 'lead_time', 'bts', 'hot', 'rft', 'final_rework']
        """
        return [member.value for member in cls if "WORD" in member.name]

    @classmethod
    def higher_better(cls):
        return {
            cls.OWE_WORD.value: True,
            cls.SAM_COST_WORD.value: False,
            cls.WIP_WORD.value: False,
            cls.LEAD_TIME_WORD.value: False,
            cls.BTS_WORD.value: True,
            cls.HOT_WORD.value: True,
            cls.RFT_WORD.value: True,
            cls.FR_WORD.value: False
        }

    @classmethod
    def metric_unit(cls):
        return {
            cls.OWE_WORD.value: '%',
            cls.SAM_COST_WORD.value: '元',
            cls.WIP_WORD.value: '天',
            cls.LEAD_TIME_WORD.value: '天',
            cls.BTS_WORD.value: '%',
            cls.HOT_WORD.value: '%',
            cls.RFT_WORD.value: '%',
            cls.FR_WORD.value: '%'
        }


if __name__ == '__main__':
    print(Metrics.get_member_en('wip'))

