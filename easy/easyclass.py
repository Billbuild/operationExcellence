import logging

import easy.constants as const
import easy.json_handle as jh
import easy.easyfunc as ef

import os
import datetime as dt
import time
import re
from typing import Literal, Tuple

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text


# *** 2025-6-24 开始***


class FastDate:

    @staticmethod
    def is_leap_year(year):
        is_leap = year % 4 == 0 and year % 100 != 0 or year % 400 == 0
        return is_leap

    @staticmethod
    def date_list_between(start_date, end_date):
        return [
            start_date + dt.timedelta(days=i)
            for i in range((end_date - start_date).days + 1)
        ]

    def __init__(self, date: dt.date):
        self.date = date
        self.validate()

    def validate(self):
        if not isinstance(self.date, dt.date):
            raise TypeError(
                f"date必须是datetime.date类型，实际收到: {type(self.date)}"
            )

    def to_str(self, purpose: Literal['hyphen', 'num']):
        if purpose == 'hyphen':
            return self.date.strftime('%Y-%m-%d')
        elif purpose == 'num':
            return self.date.strftime('%Y%m%d')
        else:
            raise TypeError(
                f"purpose 只能是 hyphen 或者 num"
            )

    def past_days(self, num):
        """
        目的：返回过去的日期，比如昨天 参数 num 为 1
        :param num:
        :return:
        """
        return self.date - dt.timedelta(days=num)

    def hyphen_date(self):
        return self.date.strftime('%Y-%m-%d')

    def month_first_day(self):
        return self.date.replace(day=1)

    def year_first_day(self):
        return self.date.replace(day=1).replace(month=1)

    def prior_month(self):
        month = self.date.month
        year = self.date.year

        if month in range(2, 13):
            month = month - 1
        if month == 1:
            month = 12
            year = year - 1

        return year, month

    def month_last_day(self):
        import calendar

        month = self.date.month
        year = self.date.year

        _, last_day = calendar.monthrange(year, month)
        return dt.date(year, month, last_day)

    def same_day_prior_month(self):
        """
        目的：计算上个月的同一天
        :return:
        """
        import calendar
        month_last_day = self.month_last_day()
        year, prior_month = self.prior_month()
        day = self.date.day
        # 如果当天是这个月的最后一天
        if day == month_last_day.day:
            # 那么通过 calendar.monthranger 获取上个月的最后一天
            _, same_day = calendar.monthrange(year, prior_month)
        else:
            same_day = day
        return dt.date(year, prior_month, same_day)

    def month_string(self):
        month = self.date.month
        if month <= 9:
            return str(0) + str(month)
        else:
            return str(month)

    def day_string(self):
        day = self.date.day
        if day <= 10:
            return str(0) + str(day)
        else:
            return str(day)

    def last_monday(self):
        weekday = self.date.weekday()
        days_to_subtract = weekday + 7
        return self.date - dt.timedelta(days=days_to_subtract)


class FastValue:

    def __init__(self, value):
        self.value = value

    def percentage(self, decimals):
        if isinstance(self.value, int) or isinstance(self.value, float):
            return str(round(self.value * 100, decimals)) + '%'
        else:
            return self.value

    def decimals(self, decimals):
        if isinstance(self.value, int) or isinstance(self.value, float):
            return str(round(self.value, decimals))
        else:
            return self.value

    def currency(self, decimals):
        if isinstance(self.value, int) or isinstance(self.value, float):
            from decimal import Decimal

            num = Decimal(str(self.value))
            formatted = "{:,.{prec}f}".format(num, prec=decimals)

            return formatted
        else:
            return self.value


class OriginAttendance:
    """
    目的：将原始的考勤数据转化为合规的考勤数据，然后上传到数据库
    """
    pass


class OriginEmployee:
    """
    目的：将原始的员工数据转化为合规的员工数据，然后上传到数据库
    """
    pass


class OriginOrganization:
    """
    目的：将原始的组织数据转化为合规的组织数据，然后上传到数据库
    """
    pass


class OriginStyle:
    """
    目的：将原始的款式数据转化为合规的款式数据，然后上传到数据库
    """
    pass


class DimensionTable:

    def __init__(self, id_value, conn):
        """

        :param id_value: 维度表的主键值
        :param conn: engine.connect()
        """
        self.id = id_value
        self._conn = conn
        self._row_data = None

    def row(self):
        if self._row_data is None:
            query = text(f"SELECT * FROM {self.tb_name} WHERE {self.id_field} = :id_value")
            result = self._conn.execute(query, {'id_value': self.id})

            row = result.fetchone()
            if not any(row):
                self._row_data = {}
            else:
                self._row_data = row._asdict()

        return self._row_data

    def all_ids(self):
        query = text(f"select {self.id_field} from {self.tb_name};")
        result = self._conn.execute(query)

        ids = [item[0] for item in result.all()]

        return ids


class Customer(DimensionTable):
    tb_name = 'customers'
    en_fields = ['id', 'name']
    cn_fields = ['客户编号', '客户名称']
    id_field = 'id'

    def __init__(self, customer_id, conn):
        super().__init__(customer_id, conn)

    @property
    def customer_name(self):
        if self._row_data is None:
            raise ValueError('请先调用 row 方法，获取正确的数据集')
        elif self._row_data == {}:
            return '--'
        else:
            return self._row_data['name']


class Employee(DimensionTable):
    tb_name = 'employees'
    en_fields = ['employee_id', 'employee_name', 'organ_id', 'position_id']
    cn_fields = ['员工编号', '员工姓名', '单位编号', '岗位编号']
    id_field = 'employee_id'

    def __init__(self, employee_id, conn):
        super().__init__(employee_id, conn)

    @property
    def employee_id(self):
        row = self.row()
        return row['employee_id']

    @property
    def organ_id(self):
        row = self.row()
        return row['organ_id']

    @property
    def position_id(self):
        row = self.row()
        return row['position_id']

    @property
    def employee_name(self):
        row = self.row()
        return row['employee_name']


class Position(DimensionTable):
    tb_name = 'positions'
    en_fields = ['position_id', 'position_name']
    cn_fields = ['岗位编号', '岗位名称']
    id_field = 'position_id'
    direct_position = ['p010', 'p011']

    def __init__(self, position_id, conn):
        super().__init__(position_id, conn)

    @property
    def position_id(self):
        row = self.row()
        return row['position_id']

    @property
    def position_name(self):
        row = self.row()
        return row['position_name']


class Style(DimensionTable):
    tb_name = 'styles'
    en_fields = ['id', 'name', 'customer_id']
    cn_fields = ['款式编号', '款式名称', '客户编号']
    id_field = 'id'

    def __init__(self, style_id, conn):
        super().__init__(style_id, conn)

    @property
    def style_id(self):
        row = self.row()
        return row['id']

    @property
    def style_name(self):
        row = self.row()
        return row['name']

    @property
    def customer_id(self):
        row = self.row()
        return row['customer_id']


class WorkingOrder(DimensionTable):
    """
    说明：
    working_order 是作为事实表存储的，但 working_order 符合通过 id 寻找记录的特性，因此作为事实表，也继承了 DimensionTable
    """
    tb_name = 'working_order'
    en_fields = ['id', 'organ_id', 'style_id', 'quantity', 'year']
    cn_fields = ['工单编号', '单位编号', '款式编号', '生产数量', '年份']
    id_field = 'id'

    def __init__(self, wo_id, conn):
        super().__init__(wo_id, conn)

    @property
    def wo_id(self):
        row = self._row_data
        return row['id']

    @property
    def style_id(self):
        row = self.row()
        return row['style_id']

    @property
    def quantity(self):
        row = self.row()
        return row['quantity']

    @property
    def organ_id(self):
        row = self.row()
        return row['organ_id']


class Organization:
    """
    说明：
    之所以没有继承 DimensionTable 是因为已经将 organizations 数据表本地化了
    方法：
    将 organization 表 本地化到 organization_config.json, 然后通过 organization_config.json 计算出关键数据
    """
    tb_name = 'organizations'
    en_fields = ['id', 'name', 'type', 'parent']
    cn_fields = ['单位编号', '单位名称', '单位类型', '上级单位序号']
    id_field = 'id'

    @staticmethod
    def organization_dic():
        return jh.load_json_as_dic(const.CONFIG_ORGANIZATION)

    @staticmethod
    def hierarchy_dic(type):
        """

        :param type: 'company'|'factory'|'workshop'
        :return:
        """
        organization_dic = Organization.organization_dic()
        hierarchy = {}
        parents = set()

        # 找到所有的 parent
        for value in organization_dic.values():
            if value['parent'] is not None:
                parents.add(value['parent'])

        if type == 'factory':
            for parent_id in parents:
                if parent_id.startswith('f'):
                    hierarchy[parent_id] = [value['id'] for value in organization_dic.values() if value['parent'] == parent_id]

        if type == 'company':
            for parent_id in parents:
                hierarchy[parent_id] = [value['id'] for value in organization_dic.values() if value['parent'] == parent_id]

        if type == 'workshop':
            pass

        return hierarchy

    def __init__(self, organ_id):
        self.organ_id = organ_id

    @property
    def name(self):
        return Organization.organization_dic()[self.organ_id]['name']

    @property
    def type(self):
        return Organization.organization_dic()[self.organ_id]['type']

    @property
    def parent(self):
        return Organization.organization_dic()[self.organ_id]['parent']

    @property
    def children_names(self):
        return [value['name'] for value in Organization.organization_dic().values() if value['parent'] == self.organ_id]

    @property
    def children_ids(self):
        return [value['id'] for value in Organization.organization_dic().values() if value['parent'] == self.organ_id]


class OrganHead(Organization):
    tb_name = 'organ_headcounts'
    en_fields = ['id', 'organ_id', 'headcount', 'direct']
    cn_fields = ['编号', '单位编码', '工人人数', '直接工人人数']

    def __init__(self, organ_id):
        super().__init__(organ_id)

    def count_head(self, conn):
        """
        目的：计算单位的员工人数，包括所有员工人数 和 直接员工人数
        问题：没有考虑到员工人数的变动
        :param conn:
        :return:
        """
        headcounts = 0
        directs = 0

        if self.type == 'company':
            query = text(f"select sum(headcount) as headcounts, sum(direct) as directs  from organ_headcounts;")
            result = conn.execute(query)

            if result.rowcount != 0:
                row = result.fetchone()
                headcounts = row.headcounts
                directs = row.directs
                indirects = headcounts - directs
            else:
                headcounts = '--'
                directs = '--'
                indirects = '--'

        if self.type == 'factory':
            query = text(f"select sum(headcount) as headcounts, sum(direct) as directs from organ_headcounts where organ_id in :children_ids;")
            result = conn.execute(query, {
                'children_ids': self.children_ids
            })

            if result.rowcount != 0:
                row = result.fetchone()
                headcounts = row.headcounts
                directs = row.directs
                indirects = headcounts - directs
            else:
                headcounts = '--'
                directs = '--'
                indirects = '--'

        if self.type == 'workshop':
            query = text(f"select sum(headcount) as headcounts, sum(direct) as directs from organ_headcounts where organ_id = :organ_id;")
            result = conn.execute(query, {
                'organ_id': self.organ_id
            })

            if result.rowcount != 0:
                row = result.fetchone()
                headcounts = row.headcounts
                directs = row.directs
                indirects = headcounts - directs
            else:
                headcounts = '--'
                directs = '--'
                indirects = '--'

        return int(headcounts), int(directs), int(indirects)

class Attendance:

    tb_name = 'attendance'
    en_fields = ['id', 'employee_id', 'date', 'morning_start', 'morning_end', 'afternoon_start', 'afternoon_end', 'overtime_start', 'overtime_end']
    cn_fields = ['考勤编号', '员工编号', '日期', '上午上班时间', '上午下班时间', '下午上班时间', '下午下班时间', '加班上班时间', '加班下班时间']

    @staticmethod
    def organ_attendance_df(conn, organ, period: Tuple[dt.date, dt.date]):
        """
        目的：获取 DataFrame，表示一段时间内，一个单位每天的工作时长（按所有打卡人员的考勤记录计算的结果）
        用途：用于生成数据表 organ_attendance
        :param conn: create_engine(const.ENGINE_OPEX).connect()
        :param organ: Organization
        :param period:
        :return:
        """
        start_date, end_date = period

        if start_date == end_date:
            # 所有车间的考勤记录
            if organ.organ_id == 'c001':
                query = text(f"select a.*, e.organ_id from attendance a join employees e on a.employee_id = e.employee_id where date = :end_date;")
            # 指定车间的考勤记录
            else:
                query = text(f"select a.*, e.organ_id from attendance a join employees e on a.employee_id = e.employee_id where e.organ_id = :organ_id and date = :end_date;")
        else:
            # 所有车间的考勤记录
            if organ.organ_id == 'c001':
                query = text(f"select a.*, e.organ_id from attendance a join employees e on a.employee_id = e.employee_id where date >= :start_date and date <= :end_date;")
            # 指定车间的考勤记录
            else:
                query = text(f"select a.*, e.organ_id from attendance a join employees e on a.employee_id = e.employee_id where e.organ_id = :organ_id and date >= :start_date and date <= :end_date;")

        result = conn.execute(query, {
            'organ_id': organ.organ_id,
            'start_date': start_date,
            'end_date': end_date
        })

        df = pd.DataFrame(result.all())

        if not df.empty:
            # 考勤字段的值包含了空值，包含空值的运算，结果是空值
            df['morning_hours'] = (df['morning_end'] - df['morning_start']).dt.total_seconds() / 3600
            df['afternoon_hours'] = (df['afternoon_end'] - df['afternoon_start']).dt.total_seconds() / 3600
            df['overtime_hours'] = (df['overtime_end'] - df['overtime_start']).dt.total_seconds() / 3600
            # 填充空值，以便得到非空的数值
            df = df.fillna(0)
            df['working_hours'] = df['morning_hours'] + df['afternoon_hours'] + df['overtime_hours']

            df = df.drop(columns=['morning_start', 'morning_end', 'afternoon_start', 'afternoon_end', 'overtime_start', 'overtime_end', 'morning_hours', 'afternoon_hours', 'overtime_hours'])

            df_group = df.groupby(['organ_id', 'date'])['working_hours'].sum().reset_index(drop=False)

            return df_group
        else:
            return df


class OrganAttendance:
    tb_name = 'organ_attendance'
    en_fields = ['id', 'organ_id', 'date', 'working_hours']
    cn_fields = ['编号', '单位编号', '日期', '工作时长']

    def __init__(self, organ):
        self.organ = organ

    def period_working_hours(self, conn, period: Tuple[dt.date, dt.date]):
        """
        目的：获取一个单位一段时期的工作时长，可以是 factory，也可以是 workshop
        :param conn:
        :param period:
        :return:
        """
        start_date, end_date = period

        if start_date == end_date:
            if self.organ.type == 'workshop':
                query = text(f"select * from organ_attendance where organ_id = :organ_id and date = :end_date;")
                result = conn.execute(query, {
                    'organ_id': self.organ.organ_id,
                    'end_date': end_date
                })
                # 判断 result 是否为空
                if result.rowcount == 0:
                    working_hours = 0
                else:
                    working_hours = result.fetchone().working_hours
                return working_hours

            if self.organ.type == 'factory':
                query = text(f"select * from organ_attendance where organ_id in :children_ids and date = :end_date")
                result = conn.execute(query, {
                    'children_ids': self.organ.children_ids,
                    'end_date': end_date
                })

                df = pd.DataFrame(result.all())
                if df.empty:
                    working_hours = 0
                else:
                    working_hours = df['working_hours'].sum()
                return working_hours

        else:
            if self.organ.type == 'workshop':
                query = text(f"select * from organ_attendance where organ_id = :organ_id and date >= :start_date and date <= :end_date;")
                result = conn.execute(query, {
                    'organ_id': self.organ.organ_id,
                    'start_date': start_date,
                    'end_date': end_date
                })
                df = pd.DataFrame(result.all())
                if df.empty:
                    working_hours = 0
                else:
                    working_hours = df['working_hours'].sum()
                return working_hours

            if self.organ.type == 'factory':
                query = text(f"select * from organ_attendance where organ_id in :children_ids and date >= :start_date and date <= :end_date;")
                result = conn.execute(query, {
                    'children_ids': self.organ.children_ids,
                    'start_date': start_date,
                    'end_date': end_date
                })
                df = pd.DataFrame(result.all())
                if df.empty:
                    working_hours = 0
                else:
                    working_hours = df['working_hours'].sum()
                return working_hours


class ManufacturingCost:
    tb_name = 'manufacturing_cost'
    en_fields = []
    cn_fields = []


class Metric:

    def __init__(self, metric: const.METRICS):
        """
        :param: metric_en: 比如 const.METRIC.OWE_EN
        """
        self.metric = metric

    @property
    def metric_name(self):
        return self.metric.value

    @property
    def metric_en_name(self):
        return const.METRICS.metric_en_dic()[self.metric_name]

    @property
    def metric_cn_name(self):
        return const.METRICS.metric_cn_dic()[self.metric_name]

    @property
    def metric_unit(self):
        return const.METRICS.metric_unit_dic()[self.metric_name]

    def high_better(self):
        return const.METRICS.high_better_dic()[self.metric_name]

    @property
    def organ_report_table(self):
        """
        目的：返回数据库中每个运营指标的报表数据表，比如 organ_owe_report, 这张表反映的是每个单位每天的 OWE 运营情况
        :return:
        """
        return f"organ_{self.metric_name}_report"


class OrganMetric(Metric):

    def __init__(self, metric: const.METRICS, organ: Organization):
        super().__init__(metric)
        self.organ = organ
        self._start_date = None
        self._end_date = None
        self._values = None

    def period(self, period: Tuple[dt.date, dt.date]):
        """
        目的：设定可以用于 period_values 的有效日期
        :param period:
        :return:
        """
        if self._start_date is None and self._end_date is None:
            self._start_date, self._end_date = period
            return self._start_date, self._end_date

    def refresh(self):
        """
        目的：初始化 self._start_date, self._end_date, self._values
        :return:
        """
        self._start_date = None
        self._end_date = None
        self._values = None
        return None

    def period_values(self):
        pass

    def run_period_values(self, period:Tuple[dt.date, dt.date]):
        """
        目的：当 period 发生变化时，依据新的 period，调用 period_values 方法
        :param period:
        :return:
        """
        if self._start_date is None and self._end_date is None and self._values is None:
            self.period(period)
            self.period_values()
        else:
            self.refresh()
            self.period(period)
            self.period_values()
    @property
    def metric_target(self):
        """
        方法：
        将 organ_metric 表 本地化到 metric_config.json, 然后通过 metric_config.json 计算出关键数据
        """
        metric_data = jh.load_json_as_dic(const.CONFIG_METRIC)

        # [...][0], [...]中只有一个元素，需要获得 scalar, 即 [...][0]
        target = [item['metric_target'] for item in metric_data if item['organization'] == self.organ.organ_id and item['metric'] == self.metric_name][0]

        return target

    @property
    def metric_report_table(self):
        """
        目的：得到运营单位运营指标日报表的名称
        :return:
        """
        return f"organ_{self.metric_name}_report"

    def is_achievable(self, value):
        if value == '--':
            return None
        else:
            if self.high_better():
                if value >= self.metric_target:
                    return True
                else:
                    return False
            else:
                if value <= self.metric_target:
                    return True
                else:
                    return False

    def value_color(self, value, purpose: Literal['html', 'js']):
        is_achievable = self.is_achievable(value)

        if is_achievable is None:
            return '--'
        else:
            if is_achievable:
                if purpose == 'js':
                    return '#4CAF50'
                if purpose == 'html':
                    return 'text-success'
            else:
                if purpose == 'js':
                    return '#F44336'
                if purpose == 'html':
                    return 'text-danger'

    def period_date_value(self, conn, period: Tuple[dt.date, dt.date]):
        """
        目的：得到两个数据组，一个是 日期数据组，一个是对应的 值数据组
        :param conn:
        :param period:
        :return: 两个元祖
        dates 示例 (datetime.date(2025, 4, 1), datetime.date(2025, 4, 2), datetime.date(2025, 4, 3))
        values 示例 (0.73032, 0.767183, 0.732083)
        """

        start_date, end_date = period
        if start_date == end_date:
            query = text(
                f"select date, {self.metric_name} from {self.organ_report_table} where organ_id = :organ_id and date = :end_date;")
            result = conn.execute(query, {
                'organ_id': self.organ.organ_id,
                'end_date': end_date
            })
        else:
            query = text(
                f"select date, {self.metric_name} from {self.organ_report_table} where organ_id = :organ_id and date >= :start_date and date <= :end_date;"
            )
            result = conn.execute(query, {
                'organ_id': self.organ.organ_id,
                'start_date': start_date,
                'end_date': end_date
            })

        # data 示例 [(dt.date(2025, 4, 1), 0.73032), (dt.date(2025, 4, 2), 0.767183), (dt.date(2025, 4, 3), 0.732083)]
        data = result.all()
        dates, values = zip(*data)
        # dates 示例 (datetime.date(2025, 4, 1), datetime.date(2025, 4, 2), datetime.date(2025, 4, 3))
        # values 示例 (0.73032, 0.767183, 0.732083)
        return dates, values


class OrganOwe(OrganMetric):
    tb_name = 'organ_owe_report'
    en_fields = ['id', 'date', 'organ_id', 'working_hours', 'sah', 'outputs', 'inspections', 'scrap']
    cn_fields = ['编号', '日期', '单位编号', '工作时长', '工时', '产量', '检验数量', '报废']

    def __init__(self, organ: Organization, conn):
        super().__init__(const.METRICS.OWE, organ)
        self.conn = conn

    def period_values(self):
        """
        前提：用 sqlalchemy 直接调用 mysql 的数据表
        :return: dict
        """

        if self._values is None:
            # 在调用方法之前，应该先通过 period 方法，设定self._start_date 和 self._end_date 的值
            if isinstance(self._start_date, dt.date) and isinstance(self._end_date, dt.date):
                row = None
                # 单个日期
                if self._start_date == self._end_date:
                    # 全公司
                    if self.organ.type == 'company':
                        query = text(
                            f"select sum(working_hourse) as working_hours, sum(sah) as sah, sum(outputs) as outputs, sum(scrap) as scrap from organ_owe_report where date = :end_date;")
                        row = self.conn.execute(query, {
                            'end_date': self._end_date
                        }).fetchone()
                    # 工厂
                    if self.organ.type == 'factory':
                        query = text(
                            f"select sum(working_hours) as working_hours, sum(sah) as sah, sum(outputs) as outputs, sum(scrap) as scrap from organ_owe_report where organ_id in :children_ids and date = :end_date;")
                        row = self.conn.execute(query, {
                            'children_ids': self.organ.children_ids,
                            'end_date': self._end_date
                        }).fetchone()
                    # 车间
                    if self.organ.type == 'workshop':
                        query = text(
                            f"select sum(working_hours) as working_hours, sum(sah) as sah, sum(outputs) as outputs, sum(scrap) as scrap from organ_owe_report where organ_id = :organ_id and date = :end_date;")
                        row = self.conn.execute(query, {
                            'organ_id': self.organ.organ_id,
                            'end_date': self._end_date
                        }).fetchone()
                else:
                    if self.organ.type == 'company':
                        query = text(
                            f"select sum(working_hours) as working_hours, sum(sah) as sah, sum(outputs) as outputs, sum(scrap) as scrap from organ_owe_report where date >= :start_date and date <= :end_date;")
                        row = self.conn.execute(query, {
                            'start_date': self._start_date,
                            'end_date': self._end_date
                        }).fetchone()
                        # python 系统 对 mysql 的数据进行除法运算时，有时会将结果转换成 Decimal 类型（精度更高），float 和 Decimal 不能直接相除
                    if self.organ.type == 'factory':
                        query = text(
                            f"select sum(working_hours) as working_hours, sum(sah) as sah, sum(outputs) as outputs, sum(scrap) as scrap from organ_owe_report where organ_id in :children_ids and date >= :start_date and date <= :end_date;")
                        row = self.conn.execute(query, {
                            'children_ids': self.organ.children_ids,
                            'start_date': self._start_date,
                            'end_date': self._end_date
                        }).fetchone()
                    if self.organ.type == 'workshop':
                        query = text(
                            f"select sum(working_hours) as working_hours, sum(sah) as sah, sum(outputs) as outputs, sum(scrap) as scrap from organ_owe_report where organ_id = :organ_id and date >= :start_date and date <= :end_date;")
                        row = self.conn.execute(query, {
                            'organ_id': self.organ.organ_id,
                            'start_date': self._start_date,
                            'end_date': self._end_date
                        }).fetchone()

                if not any(row):  # 当查询没有结果时，row 为 (None, None, None, None, ...)
                    self._values = {}
                else:
                    self._values = row._asdict()

                return self._values
            else:
                raise ValueError('请先调用 period 方法，设定正确的日期')

    @property
    def sah(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')
        elif not self._values:  # 查询为空
            return '--'
        else:
            return self._values['sah']

    @property
    def working_hours(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')
        if not self._values:  # 查询为空
            return '--'
        else:
            return self._values['working_hours']

    @property
    def outputs(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')
        elif not self._values:  # 查询为空
            return '--'
        else:
            return int(self._values['outputs'])

    @property
    def scrap(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')
        elif not self._values:  # 查询为空
            return '--'
        else:
            return int(self._values['scrap'])

    @property
    def owe(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')
        elif not self._values:  # 查询为空
            return '--'
        else:
            return self.sah / self.working_hours * (1 - self.scrap / self.outputs)


class CustomerOwe(Metric):
    """
    默认取数范围是 company
    """
    tb_name = 'style_owe_report'
    en_fields = ['id', 'date', 'style_id', 'working_hours', 'sah']
    cn_fields = ['编号', '日期', '款式编号', '工作时长', 'SAH']

    def __init__(self, customer: Customer, conn):
        super().__init__(const.METRICS.OWE)
        self.customer = customer
        self._conn = conn
        self._values = None

    def period_values(self, period: Tuple[dt.date, dt.date]):

        if self._values is None:
            start_date, end_date = period
            if start_date == end_date:
                query = text(f"select sum(r.working_hours) as working_hours, sum(r.sah) as sah from {self.tb_name} r join styles s on r.style_id = s.style_id where s.customer_id = '{self.customer.id}' and date = :end_date")
                result = self._conn.execute(query, {
                    'end_date': end_date
                })
            else:
                query = text(
                    f"select sum(r.working_hours) as working_hours, sum(r.sah) as sah from {self.tb_name} r join styles s on r.style_id = s.id where s.customer_id = '{self.customer.id}' and date >= :start_date and date <= :end_date")
                result = self._conn.execute(query, {
                    'start_date': start_date,
                    'end_date': end_date
                })

            row = result.fetchone()
            if not any(row):
                self._values = {}
            else:
                self._values = row._asdict()

            return self._values
            # if result.rowcount == 0:
            #     self._values = {}
            # else:
            #     row = result.fetchone()
            #     self._values = row._asdict()
            # return self._values

    @property
    def working_hours(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')

        if self._values == {}:
            return '--'
        else:
            return self._values['working_hours']

    @property
    def sah(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')

        if self._values == {}:
            return '--'
        else:
            return self._values['sah']

    @property
    def owe(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')

        if self._values == {}:
            return '--'
        else:
            owe = self.sah / self.working_hours
            return owe


class OrganSamCost(OrganMetric):
    def __init__(self, organ: Organization, conn):
        super().__init__(const.METRICS.SAM_COST, organ)
        self.conn = conn

    def period_values(self):
        if self._values is None:
            # 在调用方法之前，应该先通过 period 方法，设定self._start_date 和 self._end_date 的值
            if isinstance(self._start_date, dt.date) and isinstance(self._end_date, dt.date):
                row = None
                # 单个日期
                if self._start_date == self._end_date:
                    # 全公司
                    if self.organ.type == 'company':
                        query = text(
                            f"select sum(labor_cost) as labor_cost, sum(maintenance_cost) as maintenance_cost, sum(rent) as rent, sum(depreciation) as depreciation, sum(utility_cost) as utility_cost, sum(admin_cost) as admin_cost, sum(others) as others, sum(sah_m) as sah_m from organ_sam_cost_report where date = :end_date;")
                        row = self.conn.execute(query, {
                            'end_date': self._end_date
                        }).fetchone()
                    # 工厂
                    if self.organ.type == 'factory':
                        query = text(
                            f"select sum(labor_cost) as labor_cost, sum(maintenance_cost) as maintenance_cost, sum(rent) as rent, sum(depreciation) as depreciation, sum(utility_cost) as utility_cost, sum(admin_cost) as admin_cost, sum(others) as others, sum(sah_m) as sah_m from organ_sam_cost_report where organ_id in :children_ids and date = :end_date;")
                        row = self.conn.execute(query, {
                            'children_ids': self.organ.children_ids,
                            'end_date': self._end_date
                        }).fetchone()
                    # 车间
                    if self.organ.type == 'workshop':
                        query = text(
                            f"select sum(labor_cost) as labor_cost, sum(maintenance_cost) as maintenance_cost, sum(rent) as rent, sum(depreciation) as depreciation, sum(utility_cost) as utility_cost, sum(admin_cost) as admin_cost, sum(others) as others, sum(sah_m) as sah_m from organ_sam_cost_report where organ_id = :organ_id and date = :end_date;")
                        row = self.conn.execute(query, {
                            'organ_id': self.organ.organ_id,
                            'end_date': self._end_date
                        }).fetchone()
                else:
                    if self.organ.type == 'company':
                        query = text(
                            f"select sum(labor_cost) as labor_cost, sum(maintenance_cost) as maintenance_cost, sum(rent) as rent, sum(depreciation) as depreciation, sum(utility_cost) as utility_cost, sum(admin_cost) as admin_cost, sum(others) as others, sum(sah_m) as sah_m from organ_sam_cost_report where date >= :start_date and date <= :end_date;")
                        row = self.conn.execute(query, {
                            'start_date': self._start_date,
                            'end_date': self._end_date
                        }).fetchone()
                        # python 系统 对 mysql 的数据进行除法运算时，有时会将结果转换成 Decimal 类型（精度更高），float 和 Decimal 不能直接相除
                    if self.organ.type == 'factory':
                        query = text(
                            f"select sum(labor_cost) as labor_cost, sum(maintenance_cost) as maintenance_cost, sum(rent) as rent, sum(depreciation) as depreciation, sum(utility_cost) as utility_cost, sum(admin_cost) as admin_cost, sum(others) as others, sum(sah_m) as sah_m from organ_sam_cost_report where organ_id in :children_ids and date >= :start_date and date <= :end_date;")
                        row = self.conn.execute(query, {
                            'children_ids': self.organ.children_ids,
                            'start_date': self._start_date,
                            'end_date': self._end_date
                        }).fetchone()
                    if self.organ.type == 'workshop':
                        query = text(
                            f"select sum(labor_cost) as labor_cost, sum(maintenance_cost) as maintenance_cost, sum(rent) as rent, sum(depreciation) as depreciation, sum(utility_cost) as utility_cost, sum(admin_cost) as admin_cost, sum(others) as others, sum(sah_m) as sah_m from organ_sam_cost_report where organ_id = :organ_id and date >= :start_date and date <= :end_date;")
                        row = self.conn.execute(query, {
                            'organ_id': self.organ.organ_id,
                            'start_date': self._start_date,
                            'end_date': self._end_date
                        }).fetchone()

                if not any(row):  # 当查询没有结果时，row 为 (None, None, None, None, ...)
                    self._values = {}
                else:
                    self._values = row._asdict()

                return self._values
            else:
                raise ValueError('请先调用 period 方法，设定正确的日期')

    @property
    def labor_cost(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')
        elif not self._values:  # 查询为空
            return '--'
        else:
            return self._values['labor_cost']

    @property
    def maintenance_cost(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')
        elif not self._values:  # 查询为空
            return '--'
        else:
            return self._values['maintenance_cost']

    @property
    def rent(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')
        elif not self._values:  # 查询为空
            return '--'
        else:
            return self._values['rent']

    @property
    def depreciation(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')
        elif not self._values:  # 查询为空
            return '--'
        else:
            return self._values['depreciation']

    @property
    def utility_cost(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')
        elif not self._values:  # 查询为空
            return '--'
        else:
            return self._values['utility_cost']

    @property
    def admin_cost(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')
        elif not self._values:  # 查询为空
            return '--'
        else:
            return self._values['admin_cost']

    @property
    def others(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')
        elif not self._values:  # 查询为空
            return '--'
        else:
            return self._values['others']

    @property
    def sah_m(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')
        elif not self._values:  # 查询为空
            return '--'
        else:
            return self._values['sah_m']

    @property
    def manufacturing_cost(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')
        elif not self._values:  # 查询为空
            return '--'
        else:
            return self.labor_cost + self.maintenance_cost + self.rent + self.depreciation + self.utility_cost + self.admin_cost + self.others

    @property
    def sam_cost(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')
        elif not self._values:  # 查询为空
            return '--'
        else:
            return self.manufacturing_cost / self.sah_m


class OrganLeadTime(OrganMetric):
    tb_name = 'organ_lead_time_report'
    en_fields = ['id', 'organ_id', 'wo_id', 'input_offer', 'approve_offer', 'input_po', 'supplier_confirm_delivery',
                 'internal_confirm_delivery',
                 'approve_po', 'stock_in_last_bom', 'take_out_first_bom', 'product_on_line', 'product_off_line',
                 'stock_in_finished',
                 'take_out_finished', 'exit_factory_finished', 'style_id']
    cn_fields = ['id', '单位编号', '工单编号', '订单上线', '订单确认', '采购单上线', '供应商确认交期', '内部确认交期',
                 '采购单确认', '最后一件物料入库',
                 '第一件物料领用', '工单上线', '工单下线', '成品入库', '成品出库', '成品出厂', '款式编号']

    def __init__(self, organ:Organization, conn):
        super().__init__(const.METRICS.LEAD_TIME, organ)
        self.conn = conn

    def period_values(self):
        """
        说明，period 取字段 exit_factory_finished 的时间，开头是某个月的第一天，结束是某个月的最后一天
        :param conn:
        :param period:
        :return:
        """

        # cycles = {}

        if self._values is None:
            # 在调用方法之前，应该先通过 period 方法，设定self._start_date 和 self._end_date 的值
            if isinstance(self._start_date, dt.date) and isinstance(self._end_date, dt.date):
                result = None
                # 单个日期
                if self._start_date == self._end_date:
                    # 全公司
                    if self.organ.type == 'company':
                        query = text(
                            f"select * from organ_lead_time_report where exit_factory_finished = :end_date;")
                        result = self.conn.execute(query, {
                            'end_date': self._end_date
                        })
                    # 工厂
                    if self.organ.type == 'factory':
                        query = text(
                            f"select * from organ_lead_time_report where organ_id in :children_ids and exit_factory_finished = :end_date;")
                        result = self.conn.execute(query, {
                            'children_ids': self.organ.children_ids,
                            'end_date': self._end_date
                        })
                    # 车间
                    if self.organ.type == 'workshop':
                        query = text(
                            f"select * from organ_lead_time_report where organ_id = : organ_id and exit_factory_finished = :end_date;")
                        result = self.conn.execute(query, {
                            'organ_id': self.organ.organ_id,
                            'end_date': self._end_date
                        })
                else:
                    if self.organ.type == 'company':
                        query = text(
                            f"select * from organ_lead_time_report where exit_factory_finished >= :start_date and exit_factory_finished <= :end_date;")
                        result = self.conn.execute(query, {
                            'start_date': self._start_date,
                            'end_date': self._end_date
                        })
                        # python 系统 对 mysql 的数据进行除法运算时，有时会将结果转换成 Decimal 类型（精度更高），float 和 Decimal 不能直接相除
                    if self.organ.type == 'factory':
                        query = text(
                            f"select * from organ_lead_time_report where organ_id in :children_ids and exit_factory_finished >= :start_date and exit_factory_finished <= :end_date;")
                        result = self.conn.execute(query, {
                            'children_ids': self.organ.children_ids,
                            'start_date': self._start_date,
                            'end_date': self._end_date
                        })
                    if self.organ.type == 'workshop':
                        query = text(
                            f"select * from organ_lead_time_report where organ_id = :organ_id and exit_factory_finished >= :start_date and exit_factory_finished <= :end_date;")
                        row = self.conn.execute(query, {
                            'organ_id': self.organ.organ_id,
                            'start_date': self._start_date,
                            'end_date': self._end_date
                        }).fetchone()

                if result.rowcount == 0:  # 当查询没有结果时，row 为 (None, None, None, None, ...)
                    self._values = pd.DataFrame()
                else:
                    df = pd.DataFrame(result.all())
                    df['input_offer'] = pd.to_datetime(df['input_offer'])
                    df['approve_offer'] = pd.to_datetime(df['approve_offer'])
                    df['input_po'] = pd.to_datetime(df['input_po'])
                    df['supplier_confirm_delivery'] = pd.to_datetime(df['supplier_confirm_delivery'])
                    df['internal_confirm_delivery'] = pd.to_datetime(df['internal_confirm_delivery'])
                    df['approve_po'] = pd.to_datetime(df['approve_po'])
                    df['stock_in_last_bom'] = pd.to_datetime(df['stock_in_last_bom'])
                    df['take_out_first_bom'] = pd.to_datetime(df['take_out_first_bom'])
                    df['product_on_line'] = pd.to_datetime(df['product_on_line'])
                    df['product_off_line'] = pd.to_datetime(df['product_off_line'])
                    df['stock_in_finished'] = pd.to_datetime(df['stock_in_finished'])
                    df['take_out_finished'] = pd.to_datetime(df['take_out_finished'])
                    df['exit_factory_finished'] = pd.to_datetime(df['exit_factory_finished'])

                    self._values = df

                return self._values
            else:
                raise ValueError('请先调用 period 方法，设定正确的日期')

    # 前置时间
    @property
    def lead_time(self):
        if self._values.empty:
            return '--'
        else:
            df = self._values
            df['lead_time'] = (df['exit_factory_finished'] - df['input_offer']).dt.days
            lead_time = df['lead_time'].mean()
            return lead_time

    # 订单管理周期
    @property
    def offer_cycle(self):
        if self._values.empty:
            return '--'
        else:
            df = self._values
            df['offer_cycle'] = (df['approve_offer'] - df['input_offer']).dt.days
            offer_cycle = df['offer_cycle'].mean()
            return offer_cycle

    # 采购单管理周期
    @property
    def po_cycle(self):
        if self._values.empty:
            return '--'
        else:
            df = self._values
            df['po_cycle'] = (df['approve_po'] - df['input_po']).dt.days
            po_cycle = df['po_cycle'].mean()
            return po_cycle

    # 采购周期
    @property
    def delivery_cycle(self):
        if self._values.empty:
            return '--'
        else:
            df = self._values
            df['delivery_cycle'] = (df['stock_in_last_bom'] - df['approve_po']).dt.days
            delivery_cycle = df['delivery_cycle'].mean()
            return delivery_cycle

    # 车间生产周期
    @property
    def workshop_production_cycle(self):
        if self._values.empty:
            return '--'
        else:
            df = self._values
            df['workshop_production_cycle'] = (df['stock_in_finished'] - df['take_out_first_bom']).dt.days
            workshop_production_cycle = df['workshop_production_cycle'].mean()
            return workshop_production_cycle

    # 产线生产周期
    @property
    def line_product_cycle(self):
        if self._values.empty:
            return '--'
        else:
            df = self._values
            df['line_product_cycle'] = (df['product_off_line'] - df['product_on_line']).dt.days
            line_product_cycle = df['line_product_cycle'].mean()
            return line_product_cycle

    # 出货周期
    @property
    def exit_cycle(self):
        if self._values.empty:
            return '--'
        else:
            df = self._values
            df['exit_cycle'] = (df['exit_factory_finished'] - df['take_out_finished']).dt.days
            exit_cycle = df['exit_cycle'].mean()
            return exit_cycle


class WoLeadTime:
    tb_name = 'organ_lead_time_report'
    en_fields = ['id', 'organ_id', 'wo_id', 'input_offer', 'approve_offer', 'input_po', 'supplier_confirm_delivery',
                 'internal_confirm_delivery',
                 'approve_po', 'stock_in_last_bom', 'take_out_first_bom', 'product_on_line', 'product_off_line',
                 'stock_in_finished',
                 'take_out_finished', 'exit_factory_finished', 'style_id']
    cn_fields = ['id', '单位编号', '工单编号', '订单上线', '订单确认', '采购单上线', '供应商确认交期', '内部确认交期',
                 '采购单确认', '最后一件物料入库',
                 '第一件物料领用', '工单上线', '工单下线', '成品入库', '成品出库', '成品出厂', '款式编号']

    def __init__(self, wo_id, conn):
        self.conn = conn
        # 用于通过工单号调用数据
        self.wo_id = wo_id
        self._values = None

    def wo_values(self):
        if self._values is None:
            query = text(f"select * from organ_lead_time_report where wo_id = :wo_id;")
            result = self.conn.execute(query, {
                'wo_id': self.wo_id
            })

            if result.rowcount == 0:
                self._values = {}
            else:
                self._values = result.fetchone()._asdict()

        return self._values

    @property
    def organ_id(self):
        if self._values is None:
            raise ValueError('请先调用 wo_values 方法，设置正确的工单编号')

        if self._values == {}:
            return '--'
        else:
            return self._values['organ_id']

    @property
    def input_offer(self):
        if self._values is None:
            raise ValueError('请先调用 wo_values 方法，设置正确的工单编号')

        if self._values == {}:
            return '--'
        else:
            return self._values['input_offer']

    @property
    def approve_offer(self):
        if self._values is None:
            raise ValueError('请先调用 wo_values 方法，设置正确的工单编号')

        if self._values == {}:
            return '--'
        else:
            return self._values['approve_offer']

    @property
    def input_po(self):
        if self._values is None:
            raise ValueError('请先调用 wo_values 方法，设置正确的工单编号')

        if self._values == {}:
            return '--'
        else:
            return self._values['input_po']

    @property
    def supplier_confirm_delivery(self):
        if self._values is None:
            raise ValueError('请先调用 wo_values 方法，设置正确的工单编号')

        if self._values == {}:
            return '--'
        else:
            return self._values['supplier_confirm_delivery']

    @property
    def internal_confirm_delivery(self):
        if self._values is None:
            raise ValueError('请先调用 wo_values 方法，设置正确的工单编号')

        if self._values == {}:
            return '--'
        else:
            return self._values['internal_confirm_delivery']

    @property
    def approve_po(self):
        if self._values is None:
            raise ValueError('请先调用 wo_values 方法，设置正确的工单编号')

        if self._values == {}:
            return '--'
        else:
            return self._values['approve_po']

    @property
    def stock_in_last_bom(self):
        if self._values is None:
            raise ValueError('请先调用 wo_values 方法，设置正确的工单编号')

        if self._values == {}:
            return '--'
        else:
            return self._values['stock_in_last_bom']

    @property
    def take_out_first_bom(self):
        if self._values is None:
            raise ValueError('请先调用 wo_values 方法，设置正确的工单编号')

        if self._values == {}:
            return '--'
        else:
            return self._values['take_out_first_bom']

    @property
    def product_on_line(self):
        if self._values is None:
            raise ValueError('请先调用 wo_values 方法，设置正确的工单编号')

        if self._values == {}:
            return '--'
        else:
            return self._values['product_on_line']

    @property
    def product_off_line(self):
        if self._values is None:
            raise ValueError('请先调用 wo_values 方法，设置正确的工单编号')

        if self._values == {}:
            return '--'
        else:
            return self._values['product_off_line']

    @property
    def stock_in_finished(self):
        if self._values is None:
            raise ValueError('请先调用 wo_values 方法，设置正确的工单编号')

        if self._values == {}:
            return '--'
        else:
            return self._values['stock_in_finished']

    @property
    def take_out_finished(self):
        if self._values is None:
            raise ValueError('请先调用 wo_values 方法，设置正确的工单编号')

        if self._values == {}:
            return '--'
        else:
            return self._values['take_out_finished']

    @property
    def exit_factory_finished(self):
        if self._values is None:
            raise ValueError('请先调用 wo_values 方法，设置正确的工单编号')

        if self._values == {}:
            return '--'
        else:
            return self._values['exit_factory_finished']

    @property
    def style_id(self):
        if self._values is None:
            raise ValueError('请先调用 wo_values 方法，设置正确的工单编号')

        if self._values == {}:
            return '--'
        else:
            return self._values['style_id']

    @property
    def offer_cycle(self):
        if self._values is None:
            raise ValueError('请先调用 wo_values 方法，设置正确的工单编号')

        if self._values == {}:
            return '--'
        else:
            offer_cycle = self.approve_offer - self.input_offer
            return offer_cycle.days

    @property
    def po_cycle(self):
        if self._values is None:
            raise ValueError('请先调用 wo_values 方法，设置正确的工单编号')

        if self._values == {}:
            return '--'
        else:
            po_cycle = self.approve_po - self.input_po
            return po_cycle.days

    @property
    def delivery_cycle(self):
        if self._values is None:
            raise ValueError('请先调用 wo_values 方法，设置正确的工单编号')

        if self._values == {}:
            return '--'
        else:
            delivery_cycle = self.stock_in_last_bom - self.approve_po
            return delivery_cycle.days

    @property
    def workshop_production_cycle(self):
        if self._values is None:
            raise ValueError('请先调用 wo_values 方法，设置正确的工单编号')

        if self._values == {}:
            return '--'
        else:
            workshop_production_cycle = self.stock_in_finished - self.take_out_first_bom
            return workshop_production_cycle.days

    @property
    def line_product_cycle(self):
        if self._values is None:
            raise ValueError('请先调用 wo_values 方法，设置正确的工单编号')

        if self._values == {}:
            return '--'
        else:
            line_product_cycle = self.product_off_line - self.product_off_line
            return line_product_cycle.days

    @property
    def exit_cycle(self):
        if self._values is None:
            raise ValueError('请先调用 wo_values 方法，设置正确的工单编号')

        if self._values == {}:
            return '--'
        else:
            exit_cycle = self.exit_factory_finished - self.take_out_finished
            return exit_cycle.days

    @property
    def offer_waiting_days(self):
        if self._values is None:
            raise ValueError('请先调用 wo_values 方法，设置正确的工单编号')

        if self._values == {}:
            return '--'
        else:
            offer_waiting_days = self.input_po - self.approve_offer
            return offer_waiting_days.days

    @property
    def material_waiting_days(self):
        if self._values is None:
            raise ValueError('请先调用 wo_values 方法，设置正确的工单编号')

        if self._values == {}:
            return '--'
        else:
            material_waiting_days = self.take_out_first_bom - self.stock_in_last_bom
            return material_waiting_days.days

    @property
    def finished_waiting_days(self):
        if self._values is None:
            raise ValueError('请先调用 wo_values 方法，设置正确的工单编号')

        if self._values == {}:
            return '--'
        else:
            finished_waiting_days = self.take_out_finished - self.stock_in_finished
            return finished_waiting_days.days


class StyleLeadTime(Metric):
    tb_name = 'organ_lead_time_report'
    en_fields = ['id', 'organ_id', 'wo_id', 'input_offer', 'approve_offer', 'input_po', 'supplier_confirm_delivery', 'internal_confirm_delivery',
                 'approve_po', 'stock_in_last_bom', 'take_out_first_bom', 'product_on_line', 'product_off_line', 'stock_in_finished',
                 'take_out_finished', 'exit_factory_finished', 'style_id']
    cn_fields = ['id', '单位编号', '工单编号', '订单上线', '订单确认', '采购单上线', '供应商确认交期', '内部确认交期', '采购单确认', '最后一件物料入库',
                 '第一件物料领用', '工单上线', '工单下线', '成品入库', '成品出库', '成品出厂', '款式编号']

    def __init__(self, style: Style, conn):
        super().__init__(const.METRICS.LEAD_TIME)
        self.style = style
        self._conn = conn
        self._cycles = None

    def cycles(self):

        if self._cycles is None:
            cycles = {}

            query = text(f"select * from {self.tb_name} where style_id = :style_id")
            result = self._conn.execute(query, {
                'style_id': self.style.style_id
            })

            df = pd.DataFrame(result.all())

            # 改为 datetime64[ns]
            df['input_offer'] = pd.to_datetime(df['input_offer'])
            df['approve_offer'] = pd.to_datetime(df['approve_offer'])
            df['input_po'] = pd.to_datetime(df['input_po'])
            df['supplier_confirm_delivery'] = pd.to_datetime(df['supplier_confirm_delivery'])
            df['internal_confirm_delivery'] = pd.to_datetime(df['internal_confirm_delivery'])
            df['approve_po'] = pd.to_datetime(df['approve_po'])
            df['stock_in_last_bom'] = pd.to_datetime(df['stock_in_last_bom'])
            df['take_out_first_bom'] = pd.to_datetime(df['take_out_first_bom'])
            df['product_on_line'] = pd.to_datetime(df['product_on_line'])
            df['product_off_line'] = pd.to_datetime(df['product_off_line'])
            df['stock_in_finished'] = pd.to_datetime(df['stock_in_finished'])
            df['take_out_finished'] = pd.to_datetime(df['take_out_finished'])
            df['exit_factory_finished'] = pd.to_datetime(df['exit_factory_finished'])

            # 前置时间
            df['lead_time'] = (df['exit_factory_finished'] - df['input_offer']).dt.days
            lead_time = df['lead_time'].mean()
            cycles['lead_time'] = lead_time
            # 订单管理周期
            df['offer_cycle'] = (df['approve_offer'] - df['input_offer']).dt.days
            offer_cycle = df['offer_cycle'].mean()
            cycles['offer_cycle'] = offer_cycle
            # 采购单管理周期
            df['po_cycle'] = (df['approve_po'] - df['input_po']).dt.days
            po_cycle = df['po_cycle'].mean()
            cycles['po_cycle'] = po_cycle
            # 采购周期
            df['delivery_cycle'] = (df['stock_in_last_bom'] - df['approve_po']).dt.days
            delivery_cycle = df['delivery_cycle'].mean()
            cycles['delivery_cycle'] = delivery_cycle
            # 车间生产周期
            df['workshop_production_cycle'] = (df['stock_in_finished'] - df['take_out_first_bom']).dt.days
            workshop_production_cycle = df['workshop_production_cycle'].mean()
            cycles['workshop_production_cycle'] = workshop_production_cycle
            # 产线生产周期
            df['line_product_cycle'] = (df['product_off_line'] - df['product_on_line']).dt.days
            line_product_cycle = df['line_product_cycle'].mean()
            cycles['line_product_cycle'] = line_product_cycle
            # 出货周期
            df['exit_cycle'] = (df['exit_factory_finished'] - df['stock_in_finished']).dt.days
            exit_cycle = df['exit_cycle'].mean()
            cycles['exit_cycle'] = exit_cycle

            self._cycles = cycles

        return self._cycles


class CustomerLeadTime(Metric):
    """
    默认取数范围是 company
    """
    tb_name = 'organ_lead_time_report'
    en_fields = ['id', 'organ_id', 'wo_id', 'input_offer', 'approve_offer', 'input_po', 'supplier_confirm_delivery',
                 'internal_confirm_delivery',
                 'approve_po', 'stock_in_last_bom', 'take_out_first_bom', 'product_on_line', 'product_off_line',
                 'stock_in_finished',
                 'take_out_finished', 'exit_factory_finished', 'style_id']
    cn_fields = ['id', '单位编号', '工单编号', '订单上线', '订单确认', '采购单上线', '供应商确认交期', '内部确认交期',
                 '采购单确认', '最后一件物料入库',
                 '第一件物料领用', '工单上线', '工单下线', '成品入库', '成品出库', '成品出厂', '款式编号']

    def __init__(self, customer: Customer, conn):
        super().__init__(const.METRICS.LEAD_TIME)
        self.customer = customer
        self._conn = conn
        self._cycles = None

    def cycles(self, period: Tuple[dt.date, dt.date]):

        start_date, end_date = period

        if self._cycles is None:

            if start_date == end_date:
                query = text(f"select r.*, s.customer_id from {self.tb_name} r join styles s on r.style_id = s.id where s.customer_id = '{self.customer.id}' and r.exit_factory_finished = :end_date")
                result = self._conn.execute(query, {
                    'end_date': end_date,
                })
            else:
                query = text(f"select r.*, s.customer_id from {self.tb_name} r join styles s on r.style_id = s.id where s.customer_id = '{self.customer.id}' and r.exit_factory_finished >= :start_date and r.exit_factory_finished <= :end_date")
                result = self._conn.execute(query, {
                    'start_date': start_date,
                    'end_date': end_date,
                })

            df = pd.DataFrame(result.all())

            if not df.empty:
                cycles = {}

                # 改为 datetime64[ns]
                df['input_offer'] = pd.to_datetime(df['input_offer'])
                df['approve_offer'] = pd.to_datetime(df['approve_offer'])
                df['input_po'] = pd.to_datetime(df['input_po'])
                df['supplier_confirm_delivery'] = pd.to_datetime(df['supplier_confirm_delivery'])
                df['internal_confirm_delivery'] = pd.to_datetime(df['internal_confirm_delivery'])
                df['approve_po'] = pd.to_datetime(df['approve_po'])
                df['stock_in_last_bom'] = pd.to_datetime(df['stock_in_last_bom'])
                df['take_out_first_bom'] = pd.to_datetime(df['take_out_first_bom'])
                df['product_on_line'] = pd.to_datetime(df['product_on_line'])
                df['product_off_line'] = pd.to_datetime(df['product_off_line'])
                df['stock_in_finished'] = pd.to_datetime(df['stock_in_finished'])
                df['take_out_finished'] = pd.to_datetime(df['take_out_finished'])
                df['exit_factory_finished'] = pd.to_datetime(df['exit_factory_finished'])

                # 前置时间
                df['lead_time'] = (df['exit_factory_finished'] - df['input_offer']).dt.days
                lead_time = df['lead_time'].mean()
                cycles['lead_time'] = lead_time
                # # 订单管理周期
                # df['offer_cycle'] = (df['approve_offer'] - df['input_offer']).dt.days
                # offer_cycle = df['offer_cycle'].mean()
                # cycles['offer_cycle'] = offer_cycle
                # # 采购单管理周期
                # df['po_cycle'] = (df['approve_po'] - df['input_po']).dt.days
                # po_cycle = df['po_cycle'].mean()
                # cycles['po_cycle'] = po_cycle
                # # 采购周期
                # df['delivery_cycle'] = (df['stock_in_last_bom'] - df['approve_po']).dt.days
                # delivery_cycle = df['delivery_cycle'].mean()
                # cycles['delivery_cycle'] = delivery_cycle
                # # 车间生产周期
                # df['workshop_production_cycle'] = (df['stock_in_finished'] - df['take_out_first_bom']).dt.days
                # workshop_production_cycle = df['workshop_production_cycle'].mean()
                # cycles['workshop_production_cycle'] = workshop_production_cycle
                # # 产线生产周期
                # df['line_product_cycle'] = (df['product_off_line'] - df['product_on_line']).dt.days
                # line_product_cycle = df['line_product_cycle'].mean()
                # cycles['line_product_cycle'] = line_product_cycle
                # # 出货周期
                # df['exit_cycle'] = (df['exit_factory_finished'] - df['stock_in_finished']).dt.days
                # exit_cycle = df['exit_cycle'].mean()
                # cycles['exit_cycle'] = exit_cycle

                self._cycles = cycles

            else:
                self._cycles = {}

        return self._cycles

    @property
    def lead_time(self):

        if self._cycles is None:
            raise ValueError('清调用方法 cycles')

        if self._cycles == {}:
            return '--'
        else:
            return self._cycles['lead_time']


class OrganRft(OrganMetric):
    tb_name = 'organ_rft_report'
    en_fields = ['id', 'date', 'organ_id', 'wo_id', 'style_id', 'outputs', 'inspections', 'defects', 'scrap', 'fault_a', 'fault_b', 'fault_c']
    cn_fields = ['编号', '日期', '单位编号', '工单编号', '款式编号', '产量', '检验数量', '疵品数量', '报废', '疵点A', '疵点B', '疵点C']

    def __init__(self, organ: Organization, conn):
        super().__init__(const.METRICS.RFT, organ)
        self.conn = conn

    def period_values(self):
        """
        前提：用 sqlalchemy 直接调用 mysql 的数据表
        """
        if self._values is None:
            # 在调用方法之前，应该先通过 period 方法，设定self._start_date 和 self._end_date 的值
            if isinstance(self._start_date, dt.date) and isinstance(self._end_date, dt.date):
                row = None
                if self._start_date == self._end_date:
                    if self.organ.type == 'company':
                        query = text(
                            f"select sum(outputs) as outputs, sum(qualified) as qualified, sum(defects) as defects, sum(scrap) as scrap, sum(fault_a) as fault_a, sum(fault_b) as fault_b, sum(fault_c) as fault_c from organ_rft_report where date = :end_date;")
                        row = self.conn.execute(query, {
                            'organ_id': self.organ.organ_id,
                        }).fetchone()
                    if self.organ.type == 'factory':
                        query = text(
                            f"select sum(outputs) as outputs, sum(qualified) as qualified, sum(defects) as defects, sum(scrap) as scrap, sum(fault_a) as fault_a, sum(fault_b) as fault_b, sum(fault_c) as fault_c from organ_rft_report where organ_id in :children_ids and date = :end_date;"
                        )
                        row = self.conn.execute(query, {
                            'children_ids': self.organ.children_ids,
                            'end_date': self._end_date
                        }).fetchone()
                    if self.organ.type == 'workshop':
                        query = text(
                            f"select outputs, qualified, defects, scrap, fault_a, fault_b, fault_c from organ_rft_report where organ_id = :organ_id and date = :end_date;")
                        row = self.conn.execute(query, {
                            'organ_id': self.organ.organ_id,
                            'end_date': self._end_date
                        }).fetchone()
                else:
                    if self.organ.type == 'company':
                        query = text(
                            f"select sum(outputs) as outputs, sum(qualified) as qualified, sum(defects) as defects, sum(scrap) as scrap, sum(fault_a) as fault_a, sum(fault_b) as fault_b, sum(fault_c) as fault_c from organ_rft_report where date >= :start_date and date <= :end_date;")
                        row = self.conn.execute(query, {
                            'start_date': self._start_date,
                            'end_date': self._end_date
                        }).fetchone()
                    if self.organ.type == 'factory':
                        query = text(
                            f"select sum(outputs) as outputs, sum(qualified) as qualified, sum(defects) as defects, sum(scrap) as scrap, sum(fault_a) as fault_a, sum(fault_b) as fault_b, sum(fault_c) as fault_c from organ_rft_report where organ_id in :children_ids and date >= :start_date and date <= :end_date;")
                        row = self.conn.execute(query, {
                            'children_ids': self.organ.children_ids,
                            'start_date': self._start_date,
                            'end_date': self._end_date
                        }).fetchone()
                    if self.organ.type == 'workshop':
                        query = text(
                            f"select sum(outputs) as outputs, sum(qualified) as qualified, sum(defects) as defects, sum(scrap) as scrap, sum(fault_a) as fault_a, sum(fault_b) as fault_b, sum(fault_c) as fault_c from organ_rft_report where organ_id = :organ_id and date >= :start_date and date <= :end_date;")
                        row = self.conn.execute(query, {
                            'organ_id': self.organ.organ_id,
                            'start_date': self._start_date,
                            'end_date': self._end_date
                        }).fetchone()

                if not any(row):  # 当查询没有结果时，row 为 (None, None, None, None, ...)
                    self._values = {}
                else:
                    self._values = row._asdict()

                return self._values
            else:
                raise ValueError('请先调用 period 方法，设定正确的日期')

    @property
    def outputs(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')
        elif not self._values:  # 查询为空
            return '--'
        else:
            return self._values['outputs']

    @property
    def qualified(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')
        elif not self._values:  # 查询为空
            return '--'
        else:
            return self._values['qualified']

    @property
    def defects(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')
        elif not self._values:  # 查询为空
            return '--'
        else:
            return self._values['defects']

    @property
    def fault_a(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')
        elif not self._values:  # 查询为空
            return '--'
        else:
            return self._values['fault_a']

    @property
    def fault_b(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')
        elif not self._values:  # 查询为空
            return '--'
        else:
            return self._values['fault_b']

    @property
    def fault_c(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')
        elif not self._values:  # 查询为空
            return '--'
        else:
            return self._values['fault_c']

    @property
    def rft(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')
        elif not self._values:  # 查询为空
            return '--'
        else:
            rft = self.qualified / self.outputs
            return rft


class CustomerRft(Metric):
    """
    默认取数范围是 company
    """
    tb_name = 'organ_rft_report'
    en_fields = ['id', 'date', 'organ_id', 'wo_id', 'style_id', 'outputs', 'inspections', 'defects', 'scrap', 'fault_a',
                 'fault_b', 'fault_c']
    cn_fields = ['编号', '日期', '单位编号', '工单编号', '款式编号', '产量', '检验数量', '疵品数量', '报废', '疵点A',
                 '疵点B', '疵点C']

    def __init__(self, customer: Customer, conn):
        super().__init__(const.METRICS.RFT)
        self.customer = customer
        self._conn = conn
        self._values = None

    def period_values(self, period: Tuple[dt.date, dt.date]):
        start_date, end_date = period

        if self._values is None:
            values = {}

            if start_date == end_date:
                query = text(f"select r.qualified, r.outputs, s.customer_id from organ_rft_report r join styles s on r.style_id = s.id where s.customer_id = '{self.customer.id}' and date = :end_date")
                result = self._conn.execute(query, {
                    'end_date': end_date
                })
            else:
                query = text(f"select sum(r.qualified) as qualified, sum(r.outputs) as outputs, s.customer_id from organ_rft_report r join styles s on r.style_id = s.id where s.customer_id = '{self.customer.id}' and date >= :start_date and date <= :end_date")
                result = self._conn.execute(query, {
                    'start_date': start_date,
                    'end_date': end_date
                })

            if result.rowcount != 0:
                row = result.fetchone()
                values['rft'] = row.qualified / row.outputs

            self._values = values
            return self._values

    @property
    def rft(self):
        if self._values is None:
            raise ValueError('清先调用方法 period_values，获取正确的数据集')

        if self._values == {}:
            return '--'
        else:
            return self._values['rft']


class OrganFinalRework(OrganMetric):
    tb_name = 'organ_final_rework_report'
    en_fields = ['id', 'date', 'wo_id', 'style_id', 'outputs', 'qualified', 'rework', 'fault_a', 'fault_b', 'fault_c', 'organ_id', 'final_rework']
    def __init__(self, organ: Organization, conn):
        super().__init__(const.METRICS.FINAL_REWORK, organ)
        self.conn = conn

    def period_values(self):
        """
        前提：用 sqlalchemy 直接调用 mysql 的数据表
        :param conn: 是 engine.connect() as conn
        :param period:
        :return: float
        """
        if self._values is None:
            # 在调用方法之前，应该先通过 period 方法，设定self._start_date 和 self._end_date 的值
            if isinstance(self._start_date, dt.date) and isinstance(self._end_date, dt.date):
                row = None
                if self._start_date == self._end_date:
                    if self.organ.type == 'company':
                        query = text(
                            f"select sum(outputs) as outputs, sum(qualified) as qualified, sum(rework) as rework, sum(fault_a) as fault_a, sum(fault_b) as fault_b, sum(fault_c) as fault_c from organ_final_rework_report where date = :end_date;")
                        row = self.conn.execute(query, {
                            'end_date': self._end_date
                        }).fetchone()
                    if self.organ.type == 'factory':
                        query = text(
                            f"select sum(outputs) as outputs, sum(qualified) as qualified, sum(rework) as rework, sum(fault_a) as fault_a, sum(fault_b) as fault_b, sum(fault_c) as fault_c from organ_final_rework_report where organ_id in :children_ids and date = :end_date;")
                        row = self.conn.execute(query, {
                            'children_ids': self.organ.children_ids,
                            'end_date': self._end_date
                        }).fetchone()
                    if self.organ.type == 'workshop':
                        query = text(
                            f"select outputs, qualified, rework, fault_a, fault_b, fault_c from organ_final_rework_report where organ_id = :organ_id and date = :end_date;")
                        row = self.conn.execute(query, {
                            'organ_id': self.organ.organ_id,
                            'end_date': self._end_date
                        }).fetchone()
                else:
                    if self.organ.type == 'company':
                        query = text(
                            f"select sum(outputs) as outputs, sum(qualified) as qualified, sum(rework) as rework, sum(fault_a) as fault_a, sum(fault_b) as fault_b, sum(fault_c) as fault_c from organ_final_rework_report where date >= :start_date and date <= :end_date;")
                        row = self.conn.execute(query, {
                            'start_date': self._start_date,
                            'end_date': self._end_date
                        }).fetchone()
                    if self.organ.type == 'factory':
                        query = text(
                            f"select sum(outputs) as outputs, sum(qualified) as qualified, sum(rework) as rework, sum(fault_a) as fault_a, sum(fault_b) as fault_b, sum(fault_c) as fault_c from organ_final_rework_report where organ_id in :children_ids and date >= :start_date and date <= :end_date;")
                        row = self.conn.execute(query, {
                            'children_ids': self.organ.children_ids,
                            'start_date': self._start_date,
                            'end_date': self._end_date
                        }).fetchone()
                    if self.organ.type == 'workshop':
                        query = text(
                            f"select sum(outputs) as outputs, sum(qualified) as qualified, sum(rework) as rework, sum(fault_a) as fault_a, sum(fault_b) as fault_b, sum(fault_c) as fault_c from organ_final_rework_report where organ_id = :organ_id and date >= :start_date and date <= :end_date;")
                        row = self.conn.execute(query, {
                            'organ_id': self.organ.organ_id,
                            'start_date': self._start_date,
                            'end_date': self._end_date
                        }).fetchone()

                if not any(row):  # 当查询没有结果时，row 为 (None, None, None, None, ...)
                    self._values = {}
                else:
                    self._values = row._asdict()

                return self._values
            else:
                raise ValueError('请先调用 period 方法，设定正确的日期')

    @property
    def outputs(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')
        elif not self._values:  # 查询为空
            return '--'
        else:
            return self._values['outputs']

    @property
    def qualified(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')
        elif not self._values:  # 查询为空
            return '--'
        else:
            return self._values['qualified']

    @property
    def rework(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')
        elif not self._values:  # 查询为空
            return '--'
        else:
            return self._values['rework']

    @property
    def fault_a(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')
        elif not self._values:  # 查询为空
            return '--'
        else:
            return self._values['fault_a']

    @property
    def fault_b(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')
        elif not self._values:  # 查询为空
            return '--'
        else:
            return self._values['fault_b']

    @property
    def fault_c(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')
        elif not self._values:  # 查询为空
            return '--'
        else:
            return self._values['fault_c']

    @property
    def final_rework(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')
        elif not self._values:  # 查询为空
            return '--'
        else:
            final_rework = self.qualified / self.outputs
            return final_rework


class CustomerFinalRework(Metric):
    """
    默认取数范围是 company
    """
    tb_name = 'organ_final_rework_report'
    en_fields = ['id', 'date', 'wo_id', 'style_id', 'outputs', 'qualified', 'rework', 'fault_a', 'fault_b', 'fault_c',
                 'organ_id', 'final_rework']

    def __init__(self, customer: Customer, conn):
        super().__init__(const.METRICS.FINAL_REWORK)
        self.customer = customer
        self._conn = conn
        self._values = None

    def period_values(self, period: Tuple[dt.date, dt.date]):
        start_date, end_date = period

        if self._values is None:

            if start_date == end_date:
                query = text(f"select r.outputs, r.qualified, r.rework from {self.tb_name} r join styles s on r.style_id = s.id where s.customer_id = {self.customer.id} and r.date = :end_date")
                result = self._conn.execute(query, {
                    'end_date': end_date,
                })
            else:
                query = text(f"select sum(r.outputs) as outputs, sum(r.qualified) as qualified, sum(r.rework) as rework from {self.tb_name} r join styles s on r.style_id = s.id where s.customer_id = '{self.customer.id}' and r.date >= :start_date and r.date <= :end_date")
                result = self._conn.execute(query, {
                    'start_date': start_date,
                    'end_date': end_date,
                })

            if result.rowcount == 0:
                self._values = {}
            else:
                self._values = result.fetchone()._asdict()

        return self._values

    @property
    def outputs(self):
        if self._values is None:
            raise ValueError('清先调用 period_values 方法')

        if self._values == {}:
            return '--'
        else:
            return self._values['outputs']

    @property
    def qualified(self):
        if self._values is None:
            raise ValueError('清先调用 period_values 方法')

        if self._values == {}:
            return '--'
        else:
            return self._values['qualified']

    @property
    def rework(self):
        if self._values is None:
            raise ValueError('清先调用 period_values 方法')

        if self._values == {}:
            return '--'
        else:
            return self._values['rework']

    @property
    def final_rework(self):
        if self._values is None:
            raise ValueError('清先调用 period_values 方法')

        if self._values == {}:
            return '--'
        else:
            return self.rework / self.outputs


class OrganBts(OrganMetric):
    def __init__(self, organ: Organization, conn):
        super().__init__(const.METRICS.BTS, organ)
        self.conn = conn

    def period_values(self):
        """
        前提：用 sqlalchemy 直接调用 mysql 的数据表
        """

        if self._values is None:
            # 在调用方法之前，应该先通过 period 方法，设定self._start_date 和 self._end_date 的值
            if isinstance(self._start_date, dt.date) and isinstance(self._end_date, dt.date):
                row = None
                if self._start_date == self._end_date:
                    if self.organ.type == 'company':
                        query = text(
                            f"select sum(planned) as planned, sum(outputs) as outputs from organ_bts_report where date = :end_date;")
                        row = self.conn.execute(query, {
                            'end_date': self._end_date
                        }).fetchone()
                    if self.organ.type == 'factory':
                        query = text(
                            f"select sum(planned) as planned, sum(outputs) as outputs from organ_bts_report where children_ids in :children_ids and date = :end_date;")
                        row = self.conn.execute(query, {
                            'children_ids': self.organ.children_ids,
                            'end_date': self._end_date
                        }).fetchone()
                    if self.organ.type == 'workshop':
                        query = text(
                            f"select planned, outputs from organ_bts_report where organ_id = :organ_id and date = :end_date;")
                        row = self.conn.execute(query, {
                            'organ_id': self.organ.organ_id,
                            'end_date': self._end_date
                        }).fetchone()
                else:
                    if self.organ.type == 'company':
                        query = text(
                            f"select sum(planned) as planned, sum(outputs) as outputs from organ_bts_report where date >= :start_date and date <= :end_date;")
                        row = self.conn.execute(query, {
                            'start_date': self._start_date,
                            'end_date': self._end_date
                        }).fetchone()
                    if self.organ.type == 'factory':
                        query = text(
                            f"select sum(planned) as planned, sum(outputs) as outputs from organ_bts_report where organ_id in :children_ids and date >= :start_date and date <= :end_date;")
                        row = self.conn.execute(query, {
                            'children_ids': self.organ.children_ids,
                            'start_date': self._start_date,
                            'end_date': self._end_date
                        }).fetchone()
                    if self.organ.type == 'workshop':
                        query = text(
                            f"select sum(planned) as planned, sum(outputs) as outputs from organ_bts_report where organ_id = :organ_id and date >= :start_date and date <= :end_date;")
                        row = self.conn.execute(query, {
                            'organ_id': self.organ.organ_id,
                            'start_date': self._start_date,
                            'end_date': self._end_date
                        }).fetchone()

                if not any(row):  # 当查询没有结果时，row 为 (None, None, None, None, ...)
                    self._values = {}
                else:
                    self._values = row._asdict()

                return self._values
            else:
                raise ValueError('请先调用 period 方法，设定正确的日期')

    @property
    def outputs(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')
        elif not self._values:  # 查询为空
            return '--'
        else:
            return self._values['outputs']

    @property
    def planned(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')
        elif not self._values:  # 查询为空
            return '--'
        else:
            return self._values['planned']

    @property
    def bts(self):
        if self._values is None:
            raise ValueError('请先调用 period_values 方法，获取正确的数据集')
        elif not self._values:  # 查询为空
            return '--'
        else:
            bts = self.outputs / self.planned
            return bts


class CustomerBts(Metric):
    """
    暂时不做
    """

    def __init__(self, customer: Customer, conn):
        super().__init__(const.METRICS.BTS)
        self.customer = customer
        self._conn = conn
        self._values = None



# *** 2025-6-24 结束***




class OpexTable:
    def __init__(self, tb_name):
        # 事务连接的是 connection，而不是self.engine
        # self.engine = create_engine(const.ENGINE_OPEX)
        self.tb_name = tb_name
        self.unique_columns = const.CONFIG_TABLE[self.tb_name]['unique_columns']
        self.primary_key = const.CONFIG_TABLE[self.tb_name]['primary_key']

    @property
    def unique_query(self):
        """
        主要用于中间表CSV数据的上传，需要事先查询
        :return:
        """
        fields = ', '.join(list(self.unique_columns))
        return f"select {fields} from {self.tb_name};"

    def df_unique_from_sql(self, engine):
        """
        目的：查询有唯一性约束的哪些字段
        :return:
        """
        df = pd.read_sql(self.unique_query, engine)
        return df

    # 可能没意义
    def df_from_sql(self, query, engine):
        df = pd.read_sql(query, engine)
        return df

    @property
    def en2cn(self):
        dic = dict(zip(const.CONFIG_TABLE[self.tb_name]['en_fields'], const.CONFIG_TABLE[self.tb_name]['cn_fields']))
        return dic

    @property
    def cn2en(self):
        dic = dict(zip(const.CONFIG_TABLE[self.tb_name]['cn_fields'], const.CONFIG_TABLE[self.tb_name]['en_fields']))
        return dic

    def toggle_fields_name(self, li, requirement='cn2en'):
        """
        目的：实现英文字段转换为中文字段，中文字段转换为英文字段
        :param li: 中文字段列表 或者 英文字段列表
        :param requirement: 只能是两个值 'cn2en', 或者 'en2cn', 默认是 ‘cn2en'
        :return:
        """
        if requirement == 'cn2en':
            return [self.cn2en[item] for item in li]
        if requirement == 'en2cn':
            return [self.en2cn[item] for item in li]


class OpexCsv:
    def __init__(self, csv_name):
        """
        :param csv_name:
        conformity 和 origin csv文件名默认 工厂 + 主题 + 日期 + .csv, 其中日期 8位数表示年月日，6月份表示年月，4位数表示年
        csv 中间表的文件名可能没有 工厂 和 日期
        csv 文件名默认是中文
        """
        self.csv_name = csv_name

    @property
    def cn_subject(self):
        for key in const.CONFIG_CSV.keys():
            if key in self.csv_name:
                return key
        else:
            print('it can not find correct cn_subject in the csv file name.')
            return None

    @property
    def en_subject(self):
        if self.cn_subject:
            return const.CONFIG_CSV[self.cn_subject]['subject']
        else:
            print('it can not find correct en_subject in the csv file name.')
            return None

    @property
    def opex_table(self):
        return OpexTable(self.en_subject)

    @property
    def category(self):
        if self.en_subject:
            return const.CONFIG_TABLE[self.en_subject]['category']

    @property
    def factory(self):
        """
        中间表可能没有 工厂
        :return:
        """
        factory = self.csv_name.split(self.cn_subject)[0]
        if factory:
            return factory
        else:
            print(f"it can't find factory in {self.csv_name}.")
            return None

    @property
    def date_str(self):
        find_str = self.csv_name.split('.')[0]
        date_str = find_str.split(self.cn_subject)[-1]
        if date_str:
            try:
                int(date_str)
                return date_str
            except Exception as e:
                print(e)
                return None

    @property
    def hyphen_date_str(self):
        if len(self.date_str) == 8:
            return ef.transform_date_str(self.date_str)

    @property
    def year(self):
        if self.date_str:
            return self.date_str[0:4]

    @property
    def month(self):
        if self.date_str:
            if len(self.date_str) >= 6:
                return self.date_str[4:6]

    @property
    def day(self):
        if self.date_str:
            if len(self.date_str) >= 8:
                return self.date_str[6:8]

    @property
    def csv_dir(self):
        front_dir = None
        if self.category == 'conformity':
            if len(self.date_str) == 8:
                front_dir = os.path.join(const.MEASURE_DIR, 'conformity', self.en_subject, 'day')
            if len(self.date_str) == 6:
                front_dir = os.path.join(const.MEASURE_DIR, 'conformity', self.en_subject, 'month')
            if len(self.date_str) == 4:
                front_dir = os.path.join(const.MEASURE_DIR, 'conformity', self.en_subject, 'year')

            if front_dir:
                return os.path.join(front_dir, self.factory)

        if self.category == 'middle':
            pass

    @property
    def csv_path(self):
        if self.csv_dir:
            return os.path.join(self.csv_dir, self.csv_name)

    def csv_to_sql(self, engine, index=False):
        """

        :param engine:
        :param index: boolean
        :return:
        """
        df = pd.read_csv(self.csv_path)
        li_columns = df.columns.tolist()
        df.columns = self.opex_table.toggle_fields_name(li_columns, 'cn2en')
        # 当 index = False 时，df 的键值不会上传到数据库
        df.to_sql(self.en_subject, con=engine, if_exists='append', index=index)
        return True

    def csv_to_sql_norepeat(self, engine, index=False):
        """
        适合中间表，中间表经常变更内容
        CSV中的数据如果已经在表中存在，这些数据不上传
        CSV中的数据如果在表中不存在，那么这些数据上传
        :param engine
        :param index:
        :return:
        """
        # 获取CSV数据
        df_csv = pd.read_csv(self.csv_path)

        # 依据 self.unique_columns 从数据库的表获取的数据
        df_unique = self.opex_table.df_unique_from_sql(engine)

        # 判断 self.unique_columns 是否是多个列，当是单列时，生成 combine列 速度会更快
        # 如果是多个列
        if len(self.opex_table.unique_columns) > 1:
            # 只获取 self.unique_columns 的列
            df_csv_unique = df_csv[self.opex_table.unique_columns]
            # 首先将空值设置为 ''，然后获取将 self.unique_columns 列合并的值
            df_csv['combine'] = df_csv_unique.fillna('').apply(lambda row: ''.join(row), axis=1)
            df_unique['combine'] = df_unique.fillna('').apply(lambda row: ''.join(row), axis=1)
        # 如果是单个列
        else:
            df_csv['combine'] = df_csv[self.opex_table.unique_columns]
            df_unique['combine'] = df_unique[self.opex_table.unique_columns]

        # 筛选出不在表中的值
        df_csv = df_csv[~df_csv['combine'].isin(df_unique['combine'])]

        if not df_csv.empty:
            # 删除合并列
            df_csv = df_csv.drop('combine', axis='columns')
            # 打印 上传的内容
            print(df_csv)
            # 上传到数据库中的表
            df_csv.to_sql(self.en_subject, con=engine, if_exists='append', index=False)
        else:
            print('没有数据可以上传')


class OriginCsv(OpexCsv):

    @property
    def category(self):
        if super().category == 'conformity':
            return 'origin'

    @property
    def csv_dir(self):
        front_dir = None

        if len(self.date_str) == 8:
            front_dir = os.path.join(const.MEASURE_DIR, self.category, self.en_subject, 'day')
        elif len(self.date_str) == 6:
            front_dir = os.path.join(const.MEASURE_DIR, self.category, self.en_subject, 'month')
        elif len(self.date_str) == 4:
            front_dir = os.path.join(const.MEASURE_DIR, self.category, self.en_subject, 'year')
        else:
            front_dir = None

        if front_dir:
            return os.path.join(front_dir, self.factory)
        else:
            return front_dir

    def conform_attendance(self):
        """
        仅适用于考勤明细文件
        项目中需要自定义
        :return:
        """
        df = pd.read_csv(self.csv_path)
        df.insert(loc=0, column='工厂', value=self.factory)

        if self.factory == '01衢州' or self.factory == '02龙游' or self.factory == '04孟加拉':
            df['工作时长'] = df['工作时长'] / 60

        df.to_csv(OpexCsv(self.csv_name).csv_path, index=False, encoding='utf-8-sig')

    def conform_output(self):
        """
        仅适用于报工明细文件
        项目中需要自定义
        :return:
        """
        df = pd.read_csv(self.csv_path)
        df.insert(loc=1, column='工厂', value=self.factory)

        if self.factory == '01衢州' or self.factory == '02龙游':
            df = df[df['车间'] != '辅料']
        if self.factory == '03越南':
            df = df[df['车间'] != '辅料车间']
            df['车间'] = df['小组']
        if self.factory == '04孟加拉':
            df = df[df['车间'] != '辅料车间']

        df.to_csv(OpexCsv(self.csv_name).csv_path, index=False, encoding='utf-8-sig')


class ConformityLog:
    """
    目的：用于合规 csv文件 上传数据到数据库表中，在日志文件中记录相关信息
    """
    # 日志信息首先在 dic 中保存，然后持久化到 json 文件中
    dic = {}
    start = 0
    # 如果是 False，需要修改相关键对应的值，如果是 True，添加键和值
    is_dic_empty = False
    change = 0

    def __init__(self, csv_name):
        """
        目的：Conformity csv 文件上传到数据库中的日志信息
        :param csv_name:
        """
        self.csv_name = csv_name
        self.opex_csv = OpexCsv(self.csv_name)
        self.table_name = self.opex_csv.en_subject
        self.factory = self.opex_csv.factory
        self.query_date = self.opex_csv.date_str

    def is_log_table(self):
        """
        目的：判断 self.table_name 有没有名称错误，是不是一个新增的 conformity csv 的文件
        # 在 const.MEASURES_LOG_TABLES 列表中列举了需要接受csv文件数据的 表名称
        :return:
        """
        # if self.table_name in const.MEASURES_LOG_TABLES:
        if self.table_name in [table.value for table in const.Tables]:
            return True
        else:
            return False

    def initialize_dic(self):
        """
        目的：
        1 初始化 ConformityLog.dic
        2 判断 ConformityLog.dic 是否是 empty
            1）当 ConformityLog.dic 是 {} 返回 True
            2）当 ConformityLog.dic 不包括 self.table_name 返回 True
            3）当 ConformityLog.dic[self.table_name] 不包括 self.factory 返回 True
        :return:
        """
        # 因为一次有多个 .csv 文件要上传到数据库，所以，第一个文件上传的时候，需要初始化 dic
        # 不会直接在 日志文件中操作，而是在 dic 中的操作，因此初始化就是将日志文件的内容更新到 Logging.dic
        if ConformityLog.start == 0:
            # 第一个文件上传时，将 const.LOG_MEASURES_DIC 的内容更新到 ConformityLog.dic即初始化 ConformityLog.dic
            ConformityLog.dic.update(const.LOG_MEASURES_DIC)
            # start 增加 1，防止反复初始化 ConformityLog.dic
            ConformityLog.start += 1

        # 判断 ConformityLog.dic 是否存在 self.table_name 和 self.factory
        if self.table_name in ConformityLog.dic and self.factory in ConformityLog.dic[self.table_name]:
            ConformityLog.is_dic_empty = False
        else:
            ConformityLog.is_dic_empty = True

        return ConformityLog.is_dic_empty

    @property
    def logging_query_date(self):
        # 获得 ConformityLog.dic 中，上次更新的日期, latest_query_date 的格式是 yyyy-mm-dd
        return ConformityLog.dic[self.table_name][self.factory]['latest_query_date']

    def is_latest(self):
        # 说明 之前没有上传过文件，所以，这次文件上传是最近的一次上传
        if ConformityLog.is_dic_empty:
            return True
        else:
            number_logging_query_date = ef.transform_date_str(self.logging_query_date)
            if int(self.query_date) > int(number_logging_query_date):
                return True
            else:
                return False

    def change_log_dic(self):
        """
        目的：在所有csv文件上传数据后，更新日志
        :return:
        """
        # 如果是第一次上传csv文件数据

        if ConformityLog.is_dic_empty:
            ConformityLog.dic.setdefault(self.table_name, {})
            ConformityLog.dic[self.table_name].setdefault(self.factory, {})
            ConformityLog.dic[self.table_name][self.factory].setdefault('latest_query_date', ef.transform_date_str(self.query_date))
            ConformityLog.dic[self.table_name][self.factory].setdefault('log_time', ef.now_to_str(dt.datetime.now()))
        # 如果之前已经完成过csv文件数据上传
        else:
            ConformityLog.dic[self.table_name][self.factory]['latest_query_date'] = ef.transform_date_str(self.query_date)
            ConformityLog.dic[self.table_name][self.factory]['log_time'] = ef.now_to_str(dt.datetime.now())

        ConformityLog.change += 1


class AnalysisLog:
    dic = {}
    start = 0
    # 如果是 False，需要修改相关键对应的值，如果是 True，添加键和值
    is_dic_empty = False
    change = 0

    def __init__(self, tb_name, factory, query_date):
        """

        :param tb_name:
        :param factory:
        :param query_date: 字符串日期格式的列表 [yyyy-mm-dd, ]
        """
        self.tb_name = tb_name
        self.factory = factory
        self.query_date = query_date

    def is_log_table(self):
        """
        目的：判断 self.table_name 有没有名称错误，是不是一个新增的 conformity csv 的文件
        # 在 const.MEASURES_LOG_TABLES 列表中列举了需要接受csv文件数据的 表名称
        :return:
        """
        # if self.tb_name in const.MEASURES_LOG_TABLES:
        if self.tb_name in [table.value for table in const.Tables]:
            return True
        else:
            return False

    def initialize_dic(self):
        """
        目的：
        1 初始化 ConformityLog.dic
        2 判断 ConformityLog.dic 是否是 empty
            1）当 ConformityLog.dic 是 {} 返回 True
            2）当 ConformityLog.dic 不包括 self.table_name 返回 True
            3）当 ConformityLog.dic[self.table_name] 不包括 self.factory 返回 True
        :return:
        """
        # 因为一次有多个 .csv 文件要上传到数据库，所以，第一个文件上传的时候，需要初始化 dic
        # 不会直接在 日志文件中操作，而是在 dic 中的操作，因此初始化就是将日志文件的内容更新到 Logging.dic
        if self.__class__.start == 0:
            # 第一个文件上传时，将 const.LOG_MEASURES_DIC 的内容更新到 ConformityLog.dic即初始化 ConformityLog.dic
            self.__class__.dic.update(const.LOG_MEASURES_DIC)
            # start 增加 1，防止反复初始化 ConformityLog.dic
            self.__class__.start += 1

        # 判断 ConformityLog.dic 是否存在 self.table_name 和 self.factory
        if self.tb_name in self.__class__.dic and self.factory in self.__class__.dic[self.tb_name]:
            self.__class__.is_dic_empty = False
        else:
            self.__class__.is_dic_empty = True

        return self.__class__.is_dic_empty

    @property
    def logging_query_date(self):
        # 获得 ConformityLog.dic 中，上次更新的日期, latest_query_date 的格式是 yyyy-mm-dd
        return self.__class__.dic[self.tb_name][self.factory]['latest_query_date']

    def is_latest(self):
        # 说明 之前没有上传过文件，所以，这次文件上传是最近的一次上传
        if self.__class__.is_dic_empty:
            return True
        else:
            # number_logging_query_date = ef.transform_date_str(self.logging_query_date)
            # query_date 数据类型必须是 [yyyy-mm-dd, ]
            if isinstance(self.query_date, list):
                # 重置 self.query_date
                self.query_date = [date for date in self.query_date if date > self.logging_query_date]
                # 如果列表 self.query_date 的宽度为0, 说明列表为空，说明self.query_date 的时间不是最近的
                if len(self.query_date) == 0:
                    return False
                if len(self.query_date) > 0:
                    return True
            else:
                print('属性 query_date 不是列表数据类型')
                return None

    def change_log_dic(self):
        """
        目的：在所有csv文件上传数据后，更新日志
        :return:
        """
        # 如果是第一次上传csv文件数据
        end_date = max(self.query_date)
        if self.__class__.is_dic_empty:
            self.__class__.dic.setdefault(self.tb_name, {})
            self.__class__.dic[self.tb_name].setdefault(self.factory, {})
            self.__class__.dic[self.tb_name][self.factory].setdefault('latest_query_date', end_date)
            self.__class__.dic[self.tb_name][self.factory].setdefault('log_time', ef.now_to_str(dt.datetime.now()))
        # 如果之前已经完成过csv文件数据上传
        else:
            self.__class__.dic[self.tb_name][self.factory]['latest_query_date'] = end_date
            self.__class__.dic[self.tb_name][self.factory]['log_time'] = ef.now_to_str(dt.datetime.now())

        self.__class__.change += 1


class ReportLog(AnalysisLog):
    pass


class Factory:
    # dic = const.CONFIG_FACTORY
    dic = jh.load_json_as_dic(const.CONFIGS_DIR / 'factory_config.json')

    def __init__(self, factory_name=None):
        self.factory_name = factory_name

    @classmethod
    def factory_list(cls):
        """
        获取有工厂名称组成的列表 ['01衢州', '02龙游', '03越南', '04孟加拉']
        :return:
        """
        factories = []
        for item in cls.dic.keys():
            if item.startswith('_'):
                continue
            factories.append(item)
        # 因为工厂名称 是用字母开头的，比如 01衢州 02龙游,
        factories.sort()

        return factories

    @classmethod
    def factory_workshops_dic(cls):
        """

        :return: 字典 {'01衢州: ['A车间', 'B车间', ...], '02龙游': ['A车间', 'B车间', ...], ...}
        """
        return {cls.factory_list()[i]: cls.dic[cls.factory_list()[i]]['workshops'] for i in range(len(cls.factory_list()))}

    @property
    def factory_workshops(self):
        if self.factory_name is None:
            pass
        else:
            return Factory.factory_workshops_dic()[self.factory_name]


if __name__ == '__main__':

    metric = Metric(const.METRICS.SAM_COST)
    print(metric.metric_unit)