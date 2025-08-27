import easy.constants as const

import pandas as pd
import numpy as np
import datetime as dt

from sqlalchemy import create_engine, text


def organ_card_owe():
    np.random.seed(42)

    factories = {
        "01衢州": ["A车间", "B车间", "C车间", "D车间", "E车间", "常山", "姜家山", "莲花"],
        "02龙游": ["A车间", "B车间", "C车间", "D车间", "下宅车间", "十都车间", "塔石车间", "模环车间"],
        "03越南": ["A", "B", "C", "D", "E", "F", "G", "H"],
        "04孟加拉": ["1号工厂", "2号工厂", "3号工厂"]
    }
    start_date = dt.datetime(2023, 1, 1)
    end_date = dt.datetime(2025, 12, 12)
    date_range = pd.date_range(start=start_date, end=end_date)

    data = []

    for factory, workshops in factories.items():
        for workshop in workshops:
            for date in date_range:
                while True:
                    direct_labor = np.random.randint(30, 37)
                    indirect_labor = np.random.randint(4, 9)
                    direct_ratio = direct_labor / (direct_labor + indirect_labor)
                    if 0.7 <= direct_ratio <= 0.9:
                        break

                while True:
                    sah = np.random.uniform(170, 484)
                    working_hours = np.random.uniform(170, 484)
                    owe = sah / working_hours
                    if 0.4 < owe < 1:
                        break

                data.append({
                    'date': date.strftime('%Y-%m-%d'),
                    'factory': factory,
                    'workshop': workshop,
                    'direct': direct_labor,
                    'indirect': indirect_labor,
                    'direct_ratio': round(direct_ratio, 4),
                    'sah': round(sah, 2),
                    'working_hours': round(working_hours, 2),
                    'eff': round(owe, 2)
                })

    df = pd.DataFrame(data)
    print(df.shape)

    # df.to_csv(r'D:\operationExcellence\measures\zzdisplay\organ_op\card\organ_card_owe.csv', index=False, encoding='utf-8-sig')

    engine = create_engine(const.ENGINE_OPEX)
    df.to_sql('organ_card_owe', con=engine, if_exists='append', index=False)


def organ_card_sam_cost():
    np.random.seed(42)

    factories = {
        "01衢州": ["A车间", "B车间", "C车间", "D车间", "E车间", "常山", "姜家山", "莲花"],
        "02龙游": ["A车间", "B车间", "C车间", "D车间", "下宅车间", "十都车间", "塔石车间", "模环车间"],
        "03越南": ["A", "B", "C", "D", "E", "F", "G", "H"],
        "04孟加拉": ["1号工厂", "2号工厂", "3号工厂"]
    }

    start_date = dt.datetime(2023, 1, 1)
    end_date = dt.datetime(2025, 12, 12)
    date_range = pd.date_range(start=start_date, end=end_date)

    data = []

    for factory, workshops in factories.items():
        for workshop in workshops:
            for date in date_range:
                while True:
                    sah = np.random.uniform(5270, 15004)
                    working_hours = np.random.uniform(5270, 15004)
                    eff = sah / working_hours
                    if 0.4 <= eff <= 1:
                        break

                sam_cost = np.random.uniform(0.7, 0.9)

                manufacturing_cost = sah * 60 * sam_cost

                cpm = manufacturing_cost / working_hours / 60

                data.append({
                    'date': date.strftime('%Y-%m-%d'),
                    'factory': factory,
                    'workshop': workshop,
                    'sah': sah,
                    'working_hours': working_hours,
                    'eff': round(eff, 4),
                    'sam_cost': sam_cost,
                    'manufacturing_cost': manufacturing_cost,
                    'cpm': cpm
                })

    df = pd.DataFrame(data)
    print(df.shape)

    # df.to_csv(r'D:\operationExcellence\measures\zzdisplay\organ_op\card\organ_card_sam_cost.csv', index=False, encoding='utf-8-sig')

    engine = create_engine(const.ENGINE_OPEX)
    df.to_sql('organ_card_sam_cost', con=engine, if_exists='append', index=False)


def organ_card_wip():
    np.random.seed(42)

    factories = {
        "01衢州": ["A车间", "B车间", "C车间", "D车间", "E车间", "常山", "姜家山", "莲花"],
        "02龙游": ["A车间", "B车间", "C车间", "D车间", "下宅车间", "十都车间", "塔石车间", "模环车间"],
        "03越南": ["A", "B", "C", "D", "E", "F", "G", "H"],
        "04孟加拉": ["1号工厂", "2号工厂", "3号工厂"]
    }

    start_date = dt.datetime(2023, 1, 1)
    end_date = dt.datetime(2025, 12, 12)
    date_range = pd.date_range(start=start_date, end=end_date)

    data = []

    for factory, workshops in factories.items():
        for workshop in workshops:
            for date in date_range:
                avg_take_out = np.random.uniform(100000, 200001)
                wip = round(np.random.uniform(5, 15), 2)
                inventory = round(avg_take_out * wip, 2)

                data.append({
                    'date': date,
                    'factory': factory,
                    'workshop': workshop,
                    'take_out': avg_take_out,
                    'inventory': inventory,
                    'wip': wip
                })

    df = pd.DataFrame(data)
    print(df.shape)

    # df.to_csv(r'D:\operationExcellence\measures\zzdisplay\organ_op\card\organ_card_wip.csv', index=False, encoding='utf-8-sig')

    engine = create_engine(const.ENGINE_OPEX)
    df.to_sql('organ_card_wip', con=engine, if_exists='append', index=False)


def organ_card_rft():
    np.random.seed(42)

    factories = {
        "01衢州": ["A车间", "B车间", "C车间", "D车间", "E车间", "常山", "姜家山", "莲花"],
        "02龙游": ["A车间", "B车间", "C车间", "D车间", "下宅车间", "十都车间", "塔石车间", "模环车间"],
        "03越南": ["A", "B", "C", "D", "E", "F", "G", "H"],
        "04孟加拉": ["1号工厂", "2号工厂", "3号工厂"]
    }

    start_date = dt.datetime(2023, 1, 1)
    end_date = dt.datetime(2025, 12, 12)
    date_range = pd.date_range(start=start_date, end=end_date)

    data = []

    for factory, workshops in factories.items():
        for workshop in workshops:
            for date in date_range:
                output = np.random.randint(300, 501)
                rft = round(np.random.uniform(0.9, 1), 4)
                good_products = round(output * rft)
                defective_products = output - good_products

                data.append({
                    'date': date,
                    'factory': factory,
                    'workshop': workshop,
                    'outputs': output,
                    'qualified': good_products,
                    'defects': defective_products,
                    'rft': rft
                })

    df = pd.DataFrame(data)

    # df.to_csv(r'D:\operationExcellence\measures\zzdisplay\organ_op\card\organ_card_rft.csv', index=False, encoding='utf-8-sig')

    engine = create_engine(const.ENGINE_OPEX)
    df.to_sql('organ_card_rft', con=engine, if_exists='append', index=False)


def organ_card_final_rework():
    np.random.seed(42)

    factories = {
        "01衢州": ["A车间", "B车间", "C车间", "D车间", "E车间", "常山", "姜家山", "莲花"],
        "02龙游": ["A车间", "B车间", "C车间", "D车间", "下宅车间", "十都车间", "塔石车间", "模环车间"],
        "03越南": ["A", "B", "C", "D", "E", "F", "G", "H"],
        "04孟加拉": ["1号工厂", "2号工厂", "3号工厂"]
    }

    start_date = dt.datetime(2023, 1, 1)
    end_date = dt.datetime(2025, 12, 12)
    date_range = pd.date_range(start=start_date, end=end_date)

    data = []

    for factory, workshops in factories.items():
        for workshop in workshops:
            for date in date_range:
                inspections = np.random.randint(80, 150)
                defective_products = np.random.randint(0, 5)
                good_products = inspections - defective_products
                rework = defective_products / inspections

                data.append({
                    'date': date,
                    'factory': factory,
                    'workshop': workshop,
                    'inspections': inspections,
                    'qualified': good_products,
                    'defects': defective_products,
                    'rework': rework
                })

    df = pd.DataFrame(data)
    print(df.shape)

    # df.to_csv(r'D:\operationExcellence\measures\zzdisplay\organ_op\card\organ_card_rework.csv', index=False, encoding='utf-8-sig')

    engine = create_engine(const.ENGINE_OPEX)
    df.to_sql('organ_card_final_rework', con=engine, if_exists='append', index=False)


def organ_card_bts():
    np.random.seed(42)

    factories = {
        "01衢州": ["A车间", "B车间", "C车间", "D车间", "E车间", "常山", "姜家山", "莲花"],
        "02龙游": ["A车间", "B车间", "C车间", "D车间", "下宅车间", "十都车间", "塔石车间", "模环车间"],
        "03越南": ["A", "B", "C", "D", "E", "F", "G", "H"],
        "04孟加拉": ["1号工厂", "2号工厂", "3号工厂"]
    }

    start_date = dt.datetime(2023, 1, 1)
    end_date = dt.datetime(2025, 12, 12)
    date_range = pd.date_range(start=start_date, end=end_date)

    data = []

    for factory, workshops in factories.items():
        for workshop in workshops:
            for date in date_range:
                while True:
                    planned = np.random.randint(300, 501)
                    combined = np.random.randint(300, 501)
                    bts = combined / planned
                    if 0.8 <= bts <= 1:
                        break

                data.append({
                    'date': date,
                    'factory': factory,
                    'workshop': workshop,
                    'planned': planned,
                    'combined': combined,
                    'bts': bts
                })

    df = pd.DataFrame(data)
    print(df.shape)

    # df.to_csv(r'D:\operationExcellence\measures\zzdisplay\organ_op\card\organ_card_bts.csv', index=False, encoding='utf-8-sig')

    engine = create_engine(const.ENGINE_OPEX)
    df.to_sql('organ_card_bts', con=engine, if_exists='append', index=False)

def organ_card_lead_time():
    np.random.seed(42)

    factories = {
        "01衢州": ["A车间", "B车间", "C车间", "D车间", "E车间", "常山", "姜家山", "莲花"],
        "02龙游": ["A车间", "B车间", "C车间", "D车间", "下宅车间", "十都车间", "塔石车间", "模环车间"],
        "03越南": ["A", "B", "C", "D", "E", "F", "G", "H"],
        "04孟加拉": ["1号工厂", "2号工厂", "3号工厂"]
    }

    start_date = dt.datetime(2023, 1, 1)
    end_date = dt.datetime(2025, 12, 12)
    date_range = pd.date_range(start=start_date, end=end_date)

    data = []

    for factory, workshops in factories.items():
        for workshop in workshops:
            for date in date_range:
                material_taking = np.random.randint(1, 5)
                semi_products_ready = np.random.randint(1, 5)
                finished_products_ready = np.random.randint(1, 10)
                lead_time = material_taking + semi_products_ready + finished_products_ready

                data.append({
                    'date': date,
                    'factory': factory,
                    'workshop': workshop,
                    'material_taking': material_taking,
                    'semi_products_ready': semi_products_ready,
                    'finished_products_ready': finished_products_ready,
                    'lead_time': lead_time
                })

    df = pd.DataFrame(data)
    print(df.shape)

    # df.to_csv(r'D:\operationExcellence\measures\zzdisplay\organ_op\card\organ_card_lead_time.csv', index=False, encoding='utf-8-sig')

    engine = create_engine(const.ENGINE_OPEX)
    df.to_sql('organ_card_lead_time', con=engine, if_exists='append', index=False)


def organ_card_hot():
    np.random.seed(42)

    factories = {
        "01衢州": ["A车间", "B车间", "C车间", "D车间", "E车间", "常山", "姜家山", "莲花"],
        "02龙游": ["A车间", "B车间", "C车间", "D车间", "下宅车间", "十都车间", "塔石车间", "模环车间"],
        "03越南": ["A", "B", "C", "D", "E", "F", "G", "H"],
        "04孟加拉": ["1号工厂", "2号工厂", "3号工厂"]
    }

    start_date = dt.datetime(2023, 1, 1)
    end_date = dt.datetime(2025, 12, 12)
    date_range = pd.date_range(start=start_date, end=end_date)

    data = []

    for factory, workshops in factories.items():
        for workshop in workshops:
            for date in date_range:
                while True:
                    delivered = np.random.randint(1, 10)
                    delayed = np.random.randint(0, 3)
                    on_time = delivered - delayed
                    hot = on_time / delivered
                    if 0.7 <= hot <= 1:
                        break

                data.append({
                    'date': date,
                    'factory': factory,
                    'workshop': workshop,
                    'delivered': delivered,
                    'delay': delayed,
                    'on_time': on_time,
                    'hot': hot
                })

    df = pd.DataFrame(data)
    print(df.shape)

    # df.to_csv(r'D:\operationExcellence\measures\zzdisplay\organ_op\card\organ_card_hot.csv', index=False, encoding='utf-8-sig')

    engine = create_engine(const.ENGINE_OPEX)
    df.to_sql('organ_card_hot', con=engine, if_exists='append', index=False)


def organ_table_owe():
    np.random.seed(42)

    factories = {
        "01衢州": ["01衢州", "A车间", "B车间", "C车间", "D车间", "E车间", "常山", "姜家山", "莲花"],
        "02龙游": ["02龙游", "A车间", "B车间", "C车间", "D车间", "下宅车间", "十都车间", "塔石车间", "模环车间"],
        "03越南": ["03越南", "A", "B", "C", "D", "E", "F", "G", "H"],
        "04孟加拉": ["04孟加拉", "1号工厂", "2号工厂", "3号工厂"]
    }

    start_date = dt.datetime(2023, 1, 1)
    end_date = dt.datetime(2025, 12, 12)
    date_range = pd.date_range(start=start_date, end=end_date)

    data = []

    for factory, workshops in factories.items():
        for workshop in workshops:
            for date in date_range:
                mtd_eff = np.random.uniform(0.4, 1)
                ytd_eff = np.random.uniform(0.4, 1)
                mom_eff = np.random.uniform(0.4, 1)
                yoy_eff = np.random.uniform(0.4, 1)
                if workshop == factory:
                    eff = np.random.uniform(0.4, 1)
                else:
                    eff = 0

                data.append({
                    'date': date,
                    'factory': factory,
                    'workshop': workshop,
                    'eff': eff,
                    'mtd_eff': mtd_eff,
                    'ytd_eff': ytd_eff,
                    'mom_eff': mom_eff,
                    'yoy_eff': yoy_eff
                })

    df = pd.DataFrame(data)

    engine = create_engine(const.ENGINE_OPEX)
    df_card_owe = pd.read_sql(f"select * from organ_card_owe", engine)
    df_card_owe.date = pd.to_datetime(df_card_owe.date)

    df = pd.merge(df, df_card_owe[['date', 'factory', 'workshop', 'eff']], on=['date', 'factory', 'workshop'], how='left')
    df.eff_x = np.where(df.eff_x == 0, df.eff_y, df.eff_x)
    df = df.drop('eff_y', axis='columns')
    df = df.rename(columns={'eff_x': 'eff'})
    print(df.shape)

    # df.to_csv(r'D:\operationExcellence\measures\zzdisplay\organ_op\table\organ_table_owe.csv', index=False, encoding='utf-8-sig')

    df.to_sql('organ_table_owe', con=engine, if_exists='append', index=False)


def organ_table_sam_cost():
    np.random.seed(42)

    factories = {
        "01衢州": ["01衢州", "A车间", "B车间", "C车间", "D车间", "E车间", "常山", "姜家山", "莲花"],
        "02龙游": ["02龙游", "A车间", "B车间", "C车间", "D车间", "下宅车间", "十都车间", "塔石车间", "模环车间"],
        "03越南": ["03越南", "A", "B", "C", "D", "E", "F", "G", "H"],
        "04孟加拉": ["04孟加拉", "1号工厂", "2号工厂", "3号工厂"]
    }

    start_date = dt.datetime(2023, 1, 1)
    end_date = dt.datetime(2025, 12, 12)
    date_range = pd.date_range(start=start_date, end=end_date)

    data = []

    for factory, workshops in factories.items():
        for workshop in workshops:
            for date in date_range:
                mtd_sam_cost = np.random.uniform(0.7, 0.9)
                ytd_sam_cost = np.random.uniform(0.7, 0.9)
                mom_sam_cost = np.random.uniform(0.7, 0.9)
                yoy_sam_cost = np.random.uniform(0.7, 0.9)
                if workshop == factory:
                    sam_cost = np.random.uniform(0.7, 0.9)
                else:
                    sam_cost = 0

                data.append({
                    'date': date,
                    'factory': factory,
                    'workshop': workshop,
                    'sam_cost': sam_cost,
                    'mtd_sam_cost': mtd_sam_cost,
                    'ytd_sam_cost': ytd_sam_cost,
                    'mom_sam_cost': mom_sam_cost,
                    'yoy_sam_cost': yoy_sam_cost
                })

    df = pd.DataFrame(data)

    engine = create_engine(const.ENGINE_OPEX)
    df_card_sam_cost = pd.read_sql(f"select * from organ_card_sam_cost", engine)
    df_card_sam_cost.date = pd.to_datetime(df_card_sam_cost.date)

    df = pd.merge(df, df_card_sam_cost[['date', 'factory', 'workshop', 'sam_cost']], on=['date', 'factory', 'workshop'],
                  how='left')
    df.sam_cost_x = np.where(df.sam_cost_x == 0, df.sam_cost_y, df.sam_cost_x)
    df = df.drop('sam_cost_y', axis='columns')
    df = df.rename(columns={'sam_cost_x': 'sam_cost'})
    print(df.shape)

    # df.to_csv(r'D:\operationExcellence\measures\zzdisplay\organ_op\table\organ_table_sam_cost.csv', index=False, encoding='utf-8-sig')

    df.to_sql('organ_table_sam_cost', con=engine, if_exists='append', index=False)


def organ_table_wip():
    np.random.seed(42)

    factories = {
        "01衢州": ["01衢州", "A车间", "B车间", "C车间", "D车间", "E车间", "常山", "姜家山", "莲花"],
        "02龙游": ["02龙游", "A车间", "B车间", "C车间", "D车间", "下宅车间", "十都车间", "塔石车间", "模环车间"],
        "03越南": ["03越南", "A", "B", "C", "D", "E", "F", "G", "H"],
        "04孟加拉": ["04孟加拉", "1号工厂", "2号工厂", "3号工厂"]
    }

    start_date = dt.datetime(2023, 1, 1)
    end_date = dt.datetime(2025, 12, 12)
    date_range = pd.date_range(start=start_date, end=end_date)

    data = []

    for factory, workshops in factories.items():
        for workshop in workshops:
            for date in date_range:
                mtd_wip = np.random.uniform(5, 15)
                ytd_wip = np.random.uniform(5, 15)
                mom_wip = np.random.uniform(5, 15)
                yoy_wip = np.random.uniform(5, 15)
                if workshop == factory:
                    wip = np.random.uniform(5, 15)
                else:
                    wip = 0

                data.append({
                    'date': date,
                    'factory': factory,
                    'workshop': workshop,
                    'wip': wip,
                    'mtd_wip': mtd_wip,
                    'ytd_wip': ytd_wip,
                    'mom_wip': mom_wip,
                    'yoy_wip': yoy_wip
                })

    df = pd.DataFrame(data)

    engine = create_engine(const.ENGINE_OPEX)
    df_card_wip = pd.read_sql(f"select * from organ_card_wip", engine)
    df_card_wip.date = pd.to_datetime(df_card_wip.date)

    df = pd.merge(df, df_card_wip[['date', 'factory', 'workshop', 'wip']], on=['date', 'factory', 'workshop'],
                  how='left')
    df.wip_x = np.where(df.wip_x == 0, df.wip_y, df.wip_x)
    df = df.drop('wip_y', axis='columns')
    df = df.rename(columns={'wip_x': 'wip'})
    print(df.shape)

    # df.to_csv(r'D:\operationExcellence\measures\zzdisplay\organ_op\table\organ_table_wip.csv', index=False, encoding='utf-8-sig')

    df.to_sql('organ_table_wip', con=engine, if_exists='append', index=False)


def organ_table_lead_time():
    np.random.seed(42)

    factories = {
        "01衢州": ["01衢州", "A车间", "B车间", "C车间", "D车间", "E车间", "常山", "姜家山", "莲花"],
        "02龙游": ["02龙游", "A车间", "B车间", "C车间", "D车间", "下宅车间", "十都车间", "塔石车间", "模环车间"],
        "03越南": ["03越南", "A", "B", "C", "D", "E", "F", "G", "H"],
        "04孟加拉": ["04孟加拉", "1号工厂", "2号工厂", "3号工厂"]
    }

    start_date = dt.datetime(2023, 1, 1)
    end_date = dt.datetime(2025, 12, 12)
    date_range = pd.date_range(start=start_date, end=end_date)

    data = []

    for factory, workshops in factories.items():
        for workshop in workshops:
            for date in date_range:
                mtd_lead_time = np.random.uniform(3, 17)
                ytd_lead_time = np.random.uniform(3, 17)
                mom_lead_time = np.random.uniform(3, 17)
                yoy_lead_time = np.random.uniform(3, 17)
                if workshop == factory:
                    lead_time = np.random.uniform(3, 17)
                else:
                    lead_time = 0

                data.append({
                    'date': date,
                    'factory': factory,
                    'workshop': workshop,
                    'lead_time': lead_time,
                    'mtd_lead_time': mtd_lead_time,
                    'ytd_lead_time': ytd_lead_time,
                    'mom_lead_time': mom_lead_time,
                    'yoy_lead_time': yoy_lead_time
                })

    df = pd.DataFrame(data)

    engine = create_engine(const.ENGINE_OPEX)
    df_card_lead_time = pd.read_sql(f"select * from organ_card_lead_time", engine)
    df_card_lead_time.date = pd.to_datetime(df_card_lead_time.date)

    df = pd.merge(df, df_card_lead_time[['date', 'factory', 'workshop', 'lead_time']], on=['date', 'factory', 'workshop'],
                  how='left')
    df.lead_time_x = np.where(df.lead_time_x == 0, df.lead_time_y, df.lead_time_x)
    df = df.drop('lead_time_y', axis='columns')
    df = df.rename(columns={'lead_time_x': 'lead_time'})
    print(df.shape)

    # df.to_csv(r'D:\operationExcellence\measures\zzdisplay\organ_op\table\organ_table_lead_time.csv', index=False, encoding='utf-8-sig')

    df.to_sql('organ_table_lead_time', con=engine, if_exists='append', index=False)


def organ_table_bts():
    np.random.seed(42)

    factories = {
        "01衢州": ["01衢州", "A车间", "B车间", "C车间", "D车间", "E车间", "常山", "姜家山", "莲花"],
        "02龙游": ["02龙游", "A车间", "B车间", "C车间", "D车间", "下宅车间", "十都车间", "塔石车间", "模环车间"],
        "03越南": ["03越南", "A", "B", "C", "D", "E", "F", "G", "H"],
        "04孟加拉": ["04孟加拉", "1号工厂", "2号工厂", "3号工厂"]
    }

    start_date = dt.datetime(2023, 1, 1)
    end_date = dt.datetime(2025, 12, 12)
    date_range = pd.date_range(start=start_date, end=end_date)

    data = []

    for factory, workshops in factories.items():
        for workshop in workshops:
            for date in date_range:
                mtd_bts = np.random.uniform(0.8, 1)
                ytd_bts = np.random.uniform(0.8, 1)
                mom_bts = np.random.uniform(0.8, 1)
                yoy_bts = np.random.uniform(0.8, 1)
                if workshop == factory:
                    bts = np.random.uniform(0.8, 1)
                else:
                    bts = 0

                data.append({
                    'date': date,
                    'factory': factory,
                    'workshop': workshop,
                    'bts': bts,
                    'mtd_bts': mtd_bts,
                    'ytd_bts': ytd_bts,
                    'mom_bts': mom_bts,
                    'yoy_bts': yoy_bts
                })

    df = pd.DataFrame(data)

    engine = create_engine(const.ENGINE_OPEX)
    df_card_bts = pd.read_sql(f"select * from organ_card_bts", engine)
    df_card_bts.date = pd.to_datetime(df_card_bts.date)

    df = pd.merge(df, df_card_bts[['date', 'factory', 'workshop', 'bts']],
                  on=['date', 'factory', 'workshop'],
                  how='left')
    df.bts_x = np.where(df.bts_x == 0, df.bts_y, df.bts_x)
    df = df.drop('bts_y', axis='columns')
    df = df.rename(columns={'bts_x': 'bts'})
    print(df.shape)

    # df.to_csv(r'D:\operationExcellence\measures\zzdisplay\organ_op\table\organ_table_bts.csv', index=False, encoding='utf-8-sig')

    df.to_sql('organ_table_bts', con=engine, if_exists='append', index=False)


def organ_table_hot():
    np.random.seed(42)

    factories = {
        "01衢州": ["01衢州", "A车间", "B车间", "C车间", "D车间", "E车间", "常山", "姜家山", "莲花"],
        "02龙游": ["02龙游", "A车间", "B车间", "C车间", "D车间", "下宅车间", "十都车间", "塔石车间", "模环车间"],
        "03越南": ["03越南", "A", "B", "C", "D", "E", "F", "G", "H"],
        "04孟加拉": ["04孟加拉", "1号工厂", "2号工厂", "3号工厂"]
    }

    start_date = dt.datetime(2023, 1, 1)
    end_date = dt.datetime(2025, 12, 12)
    date_range = pd.date_range(start=start_date, end=end_date)

    data = []

    for factory, workshops in factories.items():
        for workshop in workshops:
            for date in date_range:
                mtd_hot = np.random.uniform(0.7, 1)
                ytd_hot = np.random.uniform(0.7, 1)
                mom_hot = np.random.uniform(0.7, 1)
                yoy_hot = np.random.uniform(0.7, 1)
                if workshop == factory:
                    hot = np.random.uniform(0.7, 1)
                else:
                    hot = 0

                data.append({
                    'date': date,
                    'factory': factory,
                    'workshop': workshop,
                    'hot': hot,
                    'mtd_hot': mtd_hot,
                    'ytd_hot': ytd_hot,
                    'mom_hot': mom_hot,
                    'yoy_hot': yoy_hot
                })

    df = pd.DataFrame(data)

    engine = create_engine(const.ENGINE_OPEX)
    df_card_hot = pd.read_sql(f"select * from organ_card_hot", engine)
    df_card_hot.date = pd.to_datetime(df_card_hot.date)

    df = pd.merge(df, df_card_hot[['date', 'factory', 'workshop', 'hot']],
                  on=['date', 'factory', 'workshop'],
                  how='left')
    df.hot_x = np.where(df.hot_x == 0, df.hot_y, df.hot_x)
    df = df.drop('hot_y', axis='columns')
    df = df.rename(columns={'hot_x': 'hot'})
    print(df.shape)

    # df.to_csv(r'D:\operationExcellence\measures\zzdisplay\organ_op\table\organ_table_hot.csv', index=False, encoding='utf-8-sig')

    df.to_sql('organ_table_hot', con=engine, if_exists='append', index=False)


def organ_table_rft():
    np.random.seed(42)

    factories = {
        "01衢州": ["01衢州", "A车间", "B车间", "C车间", "D车间", "E车间", "常山", "姜家山", "莲花"],
        "02龙游": ["02龙游", "A车间", "B车间", "C车间", "D车间", "下宅车间", "十都车间", "塔石车间", "模环车间"],
        "03越南": ["03越南", "A", "B", "C", "D", "E", "F", "G", "H"],
        "04孟加拉": ["04孟加拉", "1号工厂", "2号工厂", "3号工厂"]
    }

    start_date = dt.datetime(2023, 1, 1)
    end_date = dt.datetime(2025, 12, 12)
    date_range = pd.date_range(start=start_date, end=end_date)

    data = []

    for factory, workshops in factories.items():
        for workshop in workshops:
            for date in date_range:
                mtd_rft = np.random.uniform(0.9, 1)
                ytd_rft = np.random.uniform(0.9, 1)
                mom_rft = np.random.uniform(0.9, 1)
                yoy_rft = np.random.uniform(0.9, 1)
                if workshop == factory:
                    rft = np.random.uniform(0.9, 1)
                else:
                    rft = 0

                data.append({
                    'date': date,
                    'factory': factory,
                    'workshop': workshop,
                    'rft': rft,
                    'mtd_rft': mtd_rft,
                    'ytd_rft': ytd_rft,
                    'mom_rft': mom_rft,
                    'yoy_rft': yoy_rft
                })

    df = pd.DataFrame(data)

    engine = create_engine(const.ENGINE_OPEX)
    df_card_rft = pd.read_sql(f"select * from organ_card_rft", engine)
    df_card_rft.date = pd.to_datetime(df_card_rft.date)

    df = pd.merge(df, df_card_rft[['date', 'factory', 'workshop', 'rft']],
                  on=['date', 'factory', 'workshop'],
                  how='left')
    df.rft_x = np.where(df.rft_x == 0, df.rft_y, df.rft_x)
    df = df.drop('rft_y', axis='columns')
    df = df.rename(columns={'rft_x': 'rft'})
    print(df.shape)

    # df.to_csv(r'D:\operationExcellence\measures\zzdisplay\organ_op\table\organ_table_rft.csv', index=False, encoding='utf-8-sig')

    df.to_sql('organ_table_rft', con=engine, if_exists='append', index=False)


def organ_table_final_rework():
    np.random.seed(42)

    factories = {
        "01衢州": ["01衢州", "A车间", "B车间", "C车间", "D车间", "E车间", "常山", "姜家山", "莲花"],
        "02龙游": ["02龙游", "A车间", "B车间", "C车间", "D车间", "下宅车间", "十都车间", "塔石车间", "模环车间"],
        "03越南": ["03越南", "A", "B", "C", "D", "E", "F", "G", "H"],
        "04孟加拉": ["04孟加拉", "1号工厂", "2号工厂", "3号工厂"]
    }

    start_date = dt.datetime(2023, 1, 1)
    end_date = dt.datetime(2025, 12, 12)
    date_range = pd.date_range(start=start_date, end=end_date)

    data = []

    for factory, workshops in factories.items():
        for workshop in workshops:
            for date in date_range:
                mtd_rework = np.random.uniform(0, 0.05)
                ytd_rework = np.random.uniform(0, 0.05)
                mom_rework = np.random.uniform(0, 0.05)
                yoy_rework = np.random.uniform(0, 0.05)
                if workshop == factory:
                    rework = np.random.uniform(0, 0.05)
                else:
                    rework = 0

                data.append({
                    'date': date,
                    'factory': factory,
                    'workshop': workshop,
                    'rework': rework,
                    'mtd_rework': mtd_rework,
                    'ytd_rework': ytd_rework,
                    'mom_rework': mom_rework,
                    'yoy_rework': yoy_rework
                })

    df = pd.DataFrame(data)

    engine = create_engine(const.ENGINE_OPEX)
    df_card_final_rework = pd.read_sql(f"select * from organ_card_final_rework", engine)
    df_card_final_rework.date = pd.to_datetime(df_card_final_rework.date)

    df = pd.merge(df, df_card_final_rework[['date', 'factory', 'workshop', 'rework']],
                  on=['date', 'factory', 'workshop'],
                  how='left')
    df.rework_x = np.where(df.rework_x == 0, df.rework_y, df.rework_x)
    df = df.drop('rework_y', axis='columns')
    df = df.rename(columns={'rework_x': 'rework'})
    print(df.shape)

    # df.to_csv(r'D:\operationExcellence\measures\zzdisplay\organ_op\table\organ_table_final_rework.csv', index=False, encoding='utf-8-sig')

    df.to_sql('organ_table_final_rework', con=engine, if_exists='append', index=False)



if __name__ == '__main__':
    pass