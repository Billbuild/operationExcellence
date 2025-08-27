import easy.easyclass as ec
import easy.json_handle as jh
import easy.constants as const

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from enum import Enum


class AnalysisOWE:
    def __init__(self, start_date, end_date, factory):
        self.tb_output_name = const.Tables.OUTPUT.value
        self.tb_attendance_name = const.Tables.ATTENDANCE.value
        self.start_date = start_date
        self.end_date = end_date
        # self.query_date = query_date
        self.factory = factory
        # 定义 on_key 是因为这个衢州工厂 龙游工厂 employee_id 在报工明细和考勤明细文件中不能相互匹配, 只能有 name 匹配
        self.on_key = const.CONFIG_TABLE_DIC[self.tb_output_name]['on_key'][factory]

    def df_output(self, engine):
        """
        目的：从数据库报工明细表中读取 一天 的数据，转换成 df
        :param engine:
        :return:
        """
        output_fields = ','.join(const.CONFIG_TABLE_DIC['output']['normal_fields'])
        output_query = f"select {output_fields} from {self.tb_output_name} where factory = '{self.factory}' and date >= '{self.start_date}' and date <= '{self.end_date}';"
        df_output = pd.read_sql(output_query, engine)
        # 有可能指定的日期，工厂没有报工的数据
        if df_output.empty:
            return df_output
        else:
            df_output['sah'] = df_output['sam'] * df_output['volumes'] / 60
            return df_output

    def df_attendance(self, engine):
        """
        目的：从数据库考勤明细表中读取 一天 的数据，转换成 df
        :param engine:
        :return:
        """
        attendance_fields = ','.join(const.CONFIG_TABLE_DIC['attendance']['normal_fields'])
        # attendance_query = f"select {attendance_fields} from {self.tb_attendance_name} where factory = '{self.factory}' and date = '{self.query_date}';"
        attendance_query = f"select {attendance_fields} from {self.tb_attendance_name} where factory = '{self.factory}' and date >= '{self.start_date}' and date <= '{self.end_date}';"
        df_attendance = pd.read_sql(attendance_query, engine)
        # 有可能指定的日期工厂没有考勤数据, 所以 df_attendance 有可能是空的
        return df_attendance

    def df_organ(self, engine):
        """
        目的: 生成可以上传到数据库表 organ_owe 的 df
        :param engine:
        :return:
        """
        df_output = self.df_output(engine)
        df_attendance = self.df_attendance(engine)

        if not df_output.empty:
            df_result = df_output.groupby(['date', 'factory', 'workshop', 'line', self.on_key])['sah'].sum().reset_index(drop=False)
            df_result = pd.merge(df_result, df_attendance[['date', self.on_key, 'working_hours']], on=['date', self.on_key], how='left')
            df_result = df_result[df_result['working_hours'].notnull()]
            # 考虑到上传到数据库表 organ_owe 时，name 和 employee_id 只能取一个字段，确定取 employee_id
            df_result = df_result.rename(columns={self.on_key: 'employee_id'})
            df_result['eff'] = df_result['sah'] / df_result['working_hours']
            df_result.replace([-np.inf, np.inf], np.nan, inplace=True)
            return df_result
        else:
            return df_output

    def df_style(self, engine):
        """
        目的：生成可以上传到数据库表 style_owe 的 df
        :param engine:
        :return:
        """
        df_output = self.df_output(engine)
        df_attendance = self.df_attendance(engine)

        if not df_output.empty:
            df_style_sah = df_output.groupby(['date', 'factory', 'workshop', 'line', self.on_key, 'style_id'])['sah'].sum().reset_index(
                drop=False)
            df_style_sah = df_style_sah.rename(columns={'sah': 'style_sah'})
            df_worker_sah = df_output.groupby(['date', self.on_key])['sah'].sum().reset_index(
                drop=False)
            df_worker_sah = df_worker_sah.rename(columns={'sah': 'worker_sah'})

            df_result = pd.merge(df_style_sah, df_worker_sah, on=['date', self.on_key], how='left')
            df_result['ratio'] = df_result['style_sah'] / df_result['worker_sah']

            df_result = pd.merge(df_result, df_attendance[['date', self.on_key, 'working_hours']], on=['date', self.on_key], how='left')
            df_result['style_working_hours'] = df_result['ratio'] * df_result['working_hours']
            df_result['eff'] = df_result['style_sah'] / df_result['style_working_hours']
            # 考虑到上传到数据库表 style_owe 时，name 和 employee_id 只能取一个字段，确定取 employee_id
            df_result = df_result.rename(columns={self.on_key: 'employee_id'})
            # 将 np.inf 替换为 np.nan; 当 working_hours 为 0 时，计算 style_eff 会产生 np.inf
            df_result.replace([np.inf, -np.inf], np.nan, inplace=True)

            df_result = df_result.drop(columns=['ratio', 'worker_sah'])

            return df_result
        else:
            return df_output

    def df_to_sql(self, df_code, engine):
        if df_code in ['df_organ', 'df_style']:
            method = self.__getattribute__(df_code)
            df_result = method(engine)
            if not df_result.empty:
                if df_code == 'df_organ':
                    df_result.to_sql('organ_owe', con=engine, if_exists='append', index=False)
                if df_code == 'df_style':
                    df_result.to_sql('style_owe', con=engine, if_exists='append', index=False)


class ReportOrganOWE:
    def __init__(self, start_date, end_date, factory):
        self.start_date = start_date
        self.end_date = end_date
        self.factory = factory
        self.tb_name = const.Tables.ORGAN_OWE_REPORT.value

    def df_from_organ_owe(self, engine):
        # const.Tables.ORGAN_OWE.value 是 organ_owe
        fields = ','.join(const.CONFIG_TABLE_DIC[self.tb_name]['normal_fields'])
        report_query = f"select {fields} from {const.Tables.ORGAN_OWE.value} where factory = '{self.factory}' and date >= '{self.start_date}' and date <= '{self.end_date}';"
        df_report_owe = pd.read_sql(report_query, engine)

        return df_report_owe

    def df_organ_owe_report(self, engine):
        df = self.df_from_organ_owe(engine)

        df_factory = df.groupby(['date', 'factory']).agg({'sah': 'sum', 'working_hours': 'sum'}).reset_index(drop=False)
        df_factory.insert(loc=2, column='workshop', value=np.nan)
        df_workshop = df.groupby(['date', 'factory', 'workshop']).agg({'sah': 'sum', 'working_hours': 'sum'}).reset_index(drop=False)

        df_result = pd.concat([df_factory, df_workshop])
        df_result['eff'] = df_result['sah'] / df_result['working_hours']
        df_result.replace([-np.inf, np.inf], np.nan, inplace=True)

        return df_result

    def df_to_sql(self, engine):
        df_result = self.df_organ_owe_report(engine)
        if not df_result.empty:
            df_result.to_sql(self.tb_name, con=engine, if_exists='append', index=False)


if __name__ == '__main__':

    engine = create_engine(const.ENGINE_OPEX)
    start_date = '2024-01-01'
    end_date = '2024-01-10'

    owe_report = ReportOrganOWE(start_date, end_date, '01衢州')

    print(owe_report.tb_name)


