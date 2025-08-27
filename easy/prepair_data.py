import easy.constants as const
import easy.easyclass as ec

import os

import datetime as dt

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text


# 2025-6-24 organizations

# 维度表
# 组织架构表
# 运营指标表

def organizations():

    df = pd.read_excel(r'D:\operationExcellence\运营表单\organizations.xlsx', header=0)
    engine = create_engine(const.ENGINE_OPEX)
    df.to_sql('organizations', con=engine, if_exists='append', index=False)


def organ_metric():
    organs = ["c001", "f001", "f002", "f003", "f004", "w001", "w002", "w003", "w004", "w005", "w006", "w007", "w008", "w009", "w010", "w011", "w012", "w013",
              "w014", "w015", "w016", "w017", "w018", "w019", "w020", "w021", "w022", "w023", "w024", "w025", "w026",
              "w027", "w028", "w029", "w030", "w031", "w032"]

    metrics = ['owe', 'sam_cost', 'wip', 'lead_time', 'bts', 'hot', 'rft', 'final_rework']

    data = {}
    organization_list = []
    metric_list = []

    for unit in organs:
        for metric in metrics:
            organization_list.append(unit)
            metric_list.append(metric)

    data['organization'] = organization_list
    data['metric'] = metric_list

    df = pd.DataFrame(data)
    df['id'] = range(1, len(df) + 1)
    df['metric_target'] = np.nan

    # df.to_excel(f"D:\operationExcellence\运营表单\organ_metric.xlsx", index=False)
    engine = create_engine(const.ENGINE_OPEX)
    df.to_sql('organ_metric', con=engine, if_exists='append', index=False)


def organ_final_rework_report():
    np.random.seed(42)

    factory_hierarchy = {
        'f001': ['w001', 'w002', 'w003', 'w004', 'w005', 'w006', 'w007', 'w008'],
        'f002': ['w009', 'w010', 'w011', 'w012', 'w013', 'w014', 'w015', 'w016'],
        'f003': ['w017', 'w018', 'w019', 'w020', 'w021', 'w022', 'w023', 'w024'],
        'f004': ['w025', 'w026', 'w027', 'w028', 'w029', 'w030', 'w031', 'w032']
    }

    start_date = dt.datetime(2023, 1, 1)
    end_date = dt.datetime(2025, 12, 12)
    date_range = pd.date_range(start=start_date, end=end_date)

    data = []

    for factory_id, workshops in factory_hierarchy.items():
        for workshop_id in workshops:
            for date in date_range:
                inspections = np.random.randint(80, 150)
                defective_products = np.random.randint(0, 5)
                good_products = inspections - defective_products
                rework = defective_products / inspections

                data.append({
                    'date': date,
                    'factory': factory_id,
                    'workshop': workshop_id,
                    'inspections': inspections,
                    'qualified': good_products,
                    'defects': defective_products,
                    'final_rework': rework
                })

    df = pd.DataFrame(data)
    df_group = df.groupby(['date', 'factory']).agg({
        'inspections': 'sum', 'qualified': 'sum', 'defects': 'sum'
    }).reset_index(drop=False)
    df_group['final_rework'] = df_group['defects'] / df_group['inspections']

    df = df.rename(columns={'workshop': 'organ_id'})
    df = df.drop(columns='factory')
    df_group = df_group.rename(columns={'factory': 'organ_id'})
    df_combine = pd.concat([df_group, df]).reset_index(drop=True)

    engine = create_engine(const.ENGINE_OPEX)
    df_combine.to_sql('organ_final_rework_report', con=engine, if_exists='append', index=False)


# *** dimensions ***


def attendance():
    import random

    dimension_xlsx_path = os.path.join(const.DIR_DIMENSIONS, 'employees.xlsx')
    employees = pd.read_excel(dimension_xlsx_path, sheet_name='Sheet1')

    morning_start = dt.datetime.strptime('08:00', '%H:%M')
    morning_end = dt.datetime.strptime('12:00', '%H:%M')
    afternoon_start = dt.datetime.strptime('13:30', '%H:%M')
    afternoon_end = dt.datetime.strptime('17:30', '%H:%M')
    overtime_start = dt.datetime.strptime('18:30', '%H:%M')
    overtime_end = dt.datetime.strptime('20:30', '%H:%M')

    start_date = dt.date(2023, 1, 1)
    end_date = dt.date(2025, 12, 31)
    date_range = pd.date_range(start_date, end_date, freq='D')
    # 表示 周一到周五 0-4
    date_range = date_range[date_range.weekday < 5]

    attendance_records = []

    for date in date_range:
        date_str = date.strftime('%Y-%m-%d')
        # 加班人数不超过 1/3
        num_employees = len(employees)
        max_overtime = int(num_employees / 3)
        overtime_employees = random.sample(range(num_employees), random.randint(0, max_overtime))

        for idx, employee in employees.iterrows():
            morning_start_delta = random.randint(-10, 10)
            morning_start_time = (morning_start + dt.timedelta(minutes=morning_start_delta))
            morning_end_delta = random.randint(-10, 10)
            morning_end_time = (morning_end + dt.timedelta(minutes=morning_end_delta))

            afternoon_start_delta = random.randint(-10, 10)
            afternoon_start_time = (afternoon_start + dt.timedelta(minutes=afternoon_start_delta))
            afternoon_end_delta = random.randint(-10, 10)
            afternoon_end_time = (afternoon_end + dt.timedelta(minutes=afternoon_end_delta))

            if idx in overtime_employees:
                overtime_start_delta = random.randint(-10, 10)
                overtime_start_time = (overtime_start + dt.timedelta(minutes=overtime_start_delta))

                overtime_end_delta = random.randint(-10, 10)
                overtime_end_time = (overtime_end + dt.timedelta(minutes=overtime_end_delta))
            else:
                overtime_start_time = None
                overtime_end_time = None

            attendance_records.append({
                'employee_id': employee['employee_id'],
                'date': date_str,
                'morning_start': morning_start_time,
                'morning_end': morning_end_time,
                'afternoon_start': afternoon_start_time,
                'afternoon_end': afternoon_end_time,
                'overtime_start': overtime_start_time,
                'overtime_end': overtime_end_time
            })

    attendance_df = pd.DataFrame(attendance_records)

    engine = create_engine(const.ENGINE_OPEX)
    attendance_df.to_sql('attendance', con=engine, if_exists='append', index=False)

    # transaction_xlsx_path = os.path.join(const.DIR_TRANSACTIONS, 'attendance.xlsx')
    # attendance_df.to_excel(transaction_xlsx_path, index=False)

    # print(f"考勤记录已生成并保存到 {transaction_xlsx_path}")


def organ_attendance():
    """
    目的：应该在网页上添加这个功能，让员工的考情数据转换为车间的考勤数据
    :return:
    """

    engine = create_engine(const.ENGINE_OPEX)
    conn = engine.connect()
    # 这是一次性生成 organ_attendance, 未来有可能是每一天增加 organ_attendance 表的数据
    start_date = dt.date(2023, 1, 1)
    end_date = dt.date(2025, 12, 31)
    organ_company = ec.Organization('c001')

    df = ec.Attendance.organ_attendance_df(conn, organ_company, (start_date, end_date))

    # path = r'D:\operationExcellence\measures\analysis\organ_attendance.xlsx'
    # df.to_excel(path, index=False)

    df.to_sql('organ_attendance', con=engine, if_exists='append', index=False)
    print(df)


def steps():
    ids = [f"sp{str(i).zfill(4)}" for i in range(1, 1201)]

    names = [f"step{str(i).zfill(4)}" for i in range(1, 1201)]

    sams = np.random.uniform(0.1, 3, size=1200)

    df = pd.DataFrame({
        'id': ids,
        'name': names,
        'sam': sams
    })

    path = os.path.join(const.DIR_DIMENSIONS, 'steps.xlsx')
    df.to_excel(path, index=False)

    return df


def working_process():
    import random

    style_ids = [f"s{str(i).zfill(3)}" for i in range(1, 201)]
    all_step_ids = [f"sp{str(i).zfill(4)}" for i in range(1, 1201)]

    data = []

    for style_id in style_ids:
        num_steps = random.randint(10, 50)

        selected_steps = random.sample(all_step_ids, num_steps)

        for in_turn, step_id in enumerate(selected_steps, start=1):
            data.append({
                'style_id': style_id,
                'step_id': step_id,
                'in_turn': in_turn
            })

    working_process = pd.DataFrame(data)

    path = os.path.join(const.DIR_DIMENSIONS, 'working_process.xlsx')
    working_process.to_excel(path, index=False)


def style_sams():
    engine = create_engine(const.ENGINE_OPEX)
    query = 'select wp.*, s.sam from working_process wp join steps s on wp.step_id = s.id;'

    df = pd.read_sql(query, con=engine)

    df_group = df.groupby('style_id')['sam'].sum().reset_index(drop=False)
    df_group = df_group.rename(columns={'sam': 'sams'})

    df_group.to_sql('style_sams', con=engine, if_exists='append', index=False)


def working_order():
    """
    2023年 2024年 2025年 出勤时长一共 6545124 小时
    其中
    2023年出勤时长 2170964 小时
    2024年出勤时长 2187159 小时
    2025年出勤时长 2187000 小时
    2023 2024年 2025年产出一共 4908843 小时
    一个工单的对标产出 31人 * 10天 * 8小时 * 75% = 1860小时
    一个工单的下线是对标产出的 25%， 上线是对标产出的 300%
    :return:
    """
    import random

    np.random.seed(42)

    total_years = [2023, 2024, 2025]
    total_quantity_range = (2180000 * 0.65, 2180000 * 0.85)
    order_quantity_range = (1860 * 0.25, 1860 * 3)
    num_styles = 200
    num_orders = 3000

    order_ids = [f"wo{str(i).zfill(4)}" for i in range(1, num_orders + 1)]
    style_ids = [f"s{str(i).zfill(3)}" for i in range(1, num_styles + 1)]

    working_order = pd.DataFrame(columns=['id', 'style_id', 'quantity', 'year'])

    for year in total_years:
        year_total = random.uniform(*total_quantity_range)
        current_total = 0

        year_orders = []

        while current_total < year_total * 0.95:
            quantity = random.uniform(*order_quantity_range)

            if current_total + quantity > year_total:
                quantity = year_total - current_total

            style_id = random.choice(style_ids)

            year_orders.append({
                'style_id': style_id,
                'quantity': round(quantity, 2),
                'year': year
            })

            current_total += quantity

        year_df = pd.DataFrame(year_orders)

        start_idx = len(working_order)
        year_df['id'] = [f"wo{str(i).zfill(4)}" for i in range(start_idx + 1, start_idx + len(year_df) + 1)]

        working_order = pd.concat([working_order, year_df])

    # 这时 quantity 字段的值表示 小时，还需要转换成 数量
    working_order.reset_index(drop=True, inplace=True)

    # 读取 style_sams 数据表
    engine = create_engine(const.ENGINE_OPEX)
    query = f'select * from style_sams'
    style_sams = pd.read_sql(query, con=engine)

    # 转换成数量
    df = pd.merge(working_order, style_sams[['style_id', 'sams']], on='style_id', how='left')
    df.insert(loc=3, column='new', value=df['quantity']/df['sams']*60)
    df['new'] = df['new'].round()

    df = df.drop(columns=['quantity', 'sams'])
    df = df.rename(columns={'new': 'quantity'})

    df.to_sql('working_order', engine, if_exists='append', index=False)

    print(df.head())

    # path = os.path.join(const.DIR_TRANSACTIONS, 'working_order.xlsx')
    # working_order.to_excel(path, index=False)


def organ_headcounts():
    """
    WARNING: 每个月的员工人数和岗位都在变动，在这里没有得到反映。这里建立了一个固定不变的员工构成
    :return:
    """
    df = pd.DataFrame()
    engine = create_engine(const.ENGINE_OPEX)
    conn = engine.connect()

    query = text(f"select * from employees;")
    result = conn.execute(query)
    df = pd.DataFrame(result.all())

    if not df.empty:
        df_group = df.groupby('organ_id')['employee_id'].count().reset_index(drop=False)
        df_group = df_group.rename(columns={'employee_id': 'headcount'})

        df_direct = df[df['position_id'].isin(ec.Position.direct_position)]
        df_direct_group = df_direct.groupby('organ_id')['employee_id'].count().reset_index(drop=False)
        df_direct_group = df_direct_group.rename(columns={'employee_id': 'direct'})

        df_result = pd.merge(df_group, df_direct_group, on='organ_id', how='left')
        # ['organ_id', 'headcounts']


    # df.result.to_sql('organ_headcounts', engine, if_exists='append', index=False)
    path = os.path.join(const.DIR_ANALYSIS, 'organ_headcounts.xlsx')
    df_result.to_excel(path, index=False)




def outputs():
    """
    这个函数生成的数据并不完整，还需要在 Excel 表中做些调整，因此不要将这个函数生成的数据上传到数据库，而是要将调整后的 Excel 数据上传到数据库
    :return:
    """
    import random

    engine = create_engine(const.ENGINE_OPEX)
    with engine.connect() as conn:

        # 工单 1586个， 记录了每个工单包含的款式，工单数量以及每个款式的 sam值
        query_wo_inc_sams = "select wo.*, s.sams from working_order wo join style_sams s on wo.style_id = s.style_id;"
        wo_inc_sams = pd.read_sql(query_wo_inc_sams, conn)
        # 25056个出勤记录，783个出勤日期, 记录了每个车间每天的工作时长
        query_organ_attendance = "select * from organ_attendance;"
        organ_attendance = pd.read_sql(query_organ_attendance, conn)
        # 32个车间的员工人数
        query_organ_headcounts = "select * from organ_headcounts;"
        organ_headcounts = pd.read_sql(query_organ_headcounts, conn)

        # ['w001', 'w002', ..., 'w032']
        organ_ids = [f'w{str(i).zfill(3)}' for i in range(1, 33)]

        wo_data = wo_inc_sams[['id', 'style_id', 'quantity', 'year', 'sams']]
        wo_data.columns = ['wo_id', 'style_id', 'total_quantity', 'year', 'sams']

        output_data = []

        current_id = 1

        # 783个日期
        dates = organ_attendance['date'].unique().tolist()
        # dates_copy = dates.copy()

        wo_quantities = wo_data.shape[0]  # 工单数量 1586
        workshop_quantities = len(organ_ids)
        base_assign = wo_quantities // workshop_quantities  # 每个车间至少49个工单
        remainder = wo_quantities % workshop_quantities  # 剩余18个工单需要额外分配

        assignment = []

        # 分配订单
        for i, workshop_id in enumerate(organ_ids):
            # headcounts = organ_headcounts[organ_headcounts['organ_id'] == workshop_id]['headcount'].tolist()[0]
            working_hours = organ_attendance[organ_attendance['organ_id'] == workshop_id]['working_hours'].tolist()[0]

            # 每个车间至少分配base_assign个工单
            num_wo = base_assign
            # 前remainder个车间多分配1个工单
            if i < remainder:
                num_wo += 1

            # 获取分配给该车间的工单
            start_idx = i * base_assign + min(i, remainder)
            end_idx = start_idx + num_wo
            assigned_wos = wo_data['wo_id'][start_idx:end_idx].tolist()

            # outer_loop 是循环 for wo_id in assigned_wos:
            stop_outer_loop = False

            dates_copy = dates.copy()

            # 报工
            for wo_id in assigned_wos:

                if stop_outer_loop is True:
                    break

                line_data = wo_inc_sams[wo_inc_sams['id'] == wo_id]
                style_id = line_data.at[line_data.index[0], 'style_id']
                wo_quantity = int(line_data.at[line_data.index[0], 'quantity'])
                sams = float(line_data.at[line_data.index[0], 'sams'])

                wo_sah = wo_quantity * sams / 60

                total_quantity = 0
                while total_quantity < wo_quantity:
                    if len(dates_copy) == 0:
                        # dates_copy = dates.copy()
                        stop_outer_loop = True
                        break

                    output_date = dates_copy[0]
                    print(output_date)
                    dates_copy.remove(dates_copy[0])

                    owe = random.uniform(0.4, 1.1)
                    output_quantity = round(working_hours * owe / (sams / 60))

                    total_quantity += output_quantity

                    assignment.append({
                        'output_date': output_date,
                        'organ_id': workshop_id,
                        'wo_id': wo_id,
                        'style_id': style_id,
                        'output_quantity': output_quantity})

        df = pd.DataFrame(assignment)

        path = os.path.join(const.DIR_TRANSACTIONS, 'outputs.xlsx')
        df.to_excel(path, index=False)


def organ_rft_report():
    """
    很像是 transaction 表，但是因为 organ_outputs 表中的 每个单位每天的 wo_id 和 style_id 只有一条，从模拟的角度，将 transaction 表作为 report 表
    :return:
    """

    engine = create_engine(const.ENGINE_OPEX)
    conn = engine.connect()
    # query = text(f"select * from :table;")
    # result = conn.execute(query, {
    #     'table': 'organ_outputs'
    # })
    query = text('select * from organ_outputs;')
    result = conn.execute(query)
    df = pd.DataFrame(result.all())

    df['defects'] = (df['outputs'] * np.random.uniform(0, 0.1, size=len(df))).astype('int')
    df['qualified'] = df['outputs'] - df['defects']
    df['fault_a'] = (df['defects'] * np.random.uniform(0, 0.4)).astype('int')
    df['fault_b'] = (df['defects'] * np.random.uniform(0, 0.2)).astype('int')
    df['fault_c'] = (df['defects'] * np.random.uniform(0, 0.1)).astype('int')

    df['scrap'] = 0

    path = os.path.join(const.DIR_REPORTS, 'organ_rft_report.xlsx')
    df.to_excel(path, index=False)


def organ_owe_report():

    engine = create_engine(const.ENGINE_OPEX)
    conn = engine.connect()
    query = text(f"select a.date, a.organ_id, a.working_hours, o.style_id, o.outputs, r.inspections, r.scrap from organ_attendance a join organ_outputs o on a.date = o.date and a.organ_id = o.organ_id join organ_rft_report r on a.date = r.date and a.organ_id = r.organ_id")
    query_sams = text(f"select * from style_sams;")
    result = conn.execute(query)
    result_sams = conn.execute(query_sams)
    df = pd.DataFrame(result.all())
    df_sams = pd.DataFrame(result_sams.all())
    df = pd.merge(df, df_sams[['style_id', 'sams']], on='style_id', how='left')
    # 此处简单了，因为一个单位一天一个工单一个款式
    df['sah'] = df['outputs'] * df['sams'] / 60
    df = df.drop(columns=['style_id', 'sams'])

    path = os.path.join(const.DIR_REPORTS, 'organ_owe_report.xlsx')
    df.to_excel(path, index=False)


def style_owe_report():

    engine = create_engine(const.ENGINE_OPEX)
    conn = engine.connect()

    query = text(f"select a.date, a.organ_id, a.working_hours, o.style_id, o.outputs, s.sams from organ_outputs o join organ_attendance a on o.date = a.date and o.organ_id = a.organ_id join style_sams s on o.style_id = s.style_id")
    result = conn.execute(query)

    df = pd.DataFrame(result.all())
    df['sah'] = df['outputs'] * df['sams'] / 60
    # 不足，每天每个车间只做一个工单，即每天每个车间只做一款，现实中应该会有多个款的可能性，groupby可以不做；而且也没有按照每天sah的占比分配每个款式的working_hours
    df_group = df.groupby(['date', 'style_id']).agg({
        'working_hours': 'sum',
        'sah': 'sum'
    }).reset_index(drop=False)

    # path = os.path.join(const.DIR_REPORTS, 'style_owe_report.xlsx')
    # df_group.to_excel(path, index=False)

    df_group.to_sql('style_owe_report', con=engine, if_exists='append', index=False)


def organ_owe_report_monthly():
    engine = create_engine(const.ENGINE_OPEX)
    conn = engine.connect()

    query = "select date, organ_id, working_hours, sah, outputs, scrap from organ_owe_report;"

    df = pd.read_sql(query, con=engine)
    df['date'] = pd.to_datetime(df['date'])

    df_monthly = df.groupby([pd.Grouper(key='date', freq='ME'), 'organ_id']).agg({
        'working_hours': 'sum',
        'sah': 'sum',
        'outputs': 'sum',
        'scrap': 'sum'
    }).reset_index(drop=False)
    df_monthly['date'] = df_monthly['date'].dt.to_period('M').dt.to_timestamp()

    # path = os.path.join(const.DIR_REPORTS, 'organ_owe_report_monthly.xlsx')
    # df.to_excel(path, index=False)
    df_monthly.to_sql('organ_owe_report_monthly', engine, if_exists='append', index=False)


def organ_sam_cost_report_monthly():
    import random

    engine = create_engine(const.ENGINE_OPEX)
    conn = engine.connect()

    query = 'SELECT date, organ_id, sah FROM organ_owe_report_monthly'
    df_monthly_report = pd.read_sql(query, engine)

    cost_data = []

    for _, row in df_monthly_report.iterrows():
        sam_cost = random.uniform(0.8, 1.2)

        sah_m = row.sah * 60
        manufacturing_cost = sah_m * sam_cost
        labor_cost = manufacturing_cost * random.uniform(0.45, 0.6)
        maintenance_cost = np.random.uniform(0.05, 0.1) * manufacturing_cost
        rent = np.random.uniform(0.05, 0.1) * manufacturing_cost
        depreciation = np.random.uniform(0.05, 0.1) * manufacturing_cost
        utility_cost = np.random.uniform(0.02, 0.05) * manufacturing_cost
        admin_cost = np.random.uniform(0.02, 0.05) * manufacturing_cost
        others = manufacturing_cost - (labor_cost + maintenance_cost + rent +
                               depreciation + utility_cost + admin_cost)

        cost_data.append({
            'date': row.date,
            'organ_id': row.organ_id,
            'labor_cost': labor_cost,
            'maintenance_cost': maintenance_cost,
            'rent': rent,
            'depreciation': depreciation,
            'utility_cost': utility_cost,
            'admin_cost': admin_cost,
            'others': others,
            'sah_m': sah_m,
            'sam_cost': sam_cost
        })

    df = pd.DataFrame(cost_data)

    path = os.path.join(const.DIR_REPORTS, 'organ_sam_cost_report_monthly.xlsx')
    df.to_excel(path, index=False)


def organ_lead_time_report():
    """
    说明：organ_lead_time_report 在上传到数据库之后，又增加了字段 style_id的内容
    :return:
    """
    engine = create_engine(const.ENGINE_OPEX)

    df = pd.read_sql('organ_outputs', con=engine)

    result = df.groupby(['organ_id', 'wo_id']).agg(
        product_on_line=('date', min),
        product_off_line=('date', max)
    ).reset_index(drop=False)

    result['take_out_first_bom'] = result['product_on_line'].apply(
        lambda x: x - dt.timedelta(days=np.random.randint(0, 8)))
    result['stock_in_last_bom'] = result['product_on_line'].apply(
        lambda x: x - dt.timedelta(days=np.random.randint(0, 15)))
    result['approve_po'] = result['stock_in_last_bom'].apply(lambda x: x - dt.timedelta(days=np.random.randint(20, 61)))
    result['internal_confirm_delivery'] = result['approve_po'].apply(
        lambda x: x - dt.timedelta(days=np.random.randint(0, 4)))
    result['supplier_confirm_delivery'] = result['internal_confirm_delivery'].apply(
        lambda x: x - dt.timedelta(days=np.random.randint(0, 4)))
    result['input_po'] = result['supplier_confirm_delivery'].apply(
        lambda x: x - dt.timedelta(days=np.random.randint(0, 8)))
    result['approve_offer'] = result['input_po'].apply(lambda x: x - dt.timedelta(days=np.random.randint(0, 4)))
    result['input_offer'] = result['approve_offer'].apply(lambda x: x - dt.timedelta(days=np.random.randint(0, 4)))
    result['stock_in_finished'] = result['product_off_line'].apply(
        lambda x: x + dt.timedelta(days=np.random.randint(0, 8)))
    result['take_out_finished'] = result['stock_in_finished'].apply(
        lambda x: x + dt.timedelta(days=np.random.randint(0, 20)))
    result['exit_factory_finished'] = result['take_out_finished'].apply(
        lambda x: x + dt.timedelta(days=np.random.randint(0, 3)))

    result = result[['organ_id', 'wo_id', 'input_offer', 'approve_offer', 'input_po', 'supplier_confirm_delivery',
                     'internal_confirm_delivery', 'approve_po', 'stock_in_last_bom', 'take_out_first_bom',
                     'product_on_line', 'product_off_line', 'stock_in_finished', 'take_out_finished',
                     'exit_factory_finished']]

    path = os.path.join(const.DIR_REPORTS, 'organ_lead_time_report.xlsx')
    result.to_excel(path, index=False)


def organ_bts():
    engine = create_engine(const.ENGINE_OPEX)
    conn = engine.connect()

    query = text(
        f"select o.*, c.isocalendar_year, c.week, c.weekday from organ_outputs o left join calendar c on o.date = c.date")
    query_calendar = text(f"select * from calendar")

    result = conn.execute(query)
    df = pd.DataFrame(result.all())
    result_calendar = conn.execute(query_calendar)
    df_calendar = pd.DataFrame(result_calendar.all())

    df_bts = df.groupby(['organ_id', 'wo_id', 'isocalendar_year', 'week'])['outputs'].sum().reset_index(drop=False)
    df_bts['planned'] = (df_bts['outputs'] * np.random.uniform(0.85, 1.1, size=len(df_bts))).astype('int')

    df_week_zero = df_calendar[df_calendar['weekday'] == 0]
    df_bts = pd.merge(df_bts, df_week_zero[['date', 'isocalendar_year', 'week']], how='left',
                      on=['isocalendar_year', 'week'])

    path = os.path.join(const.DIR_REPORTS, 'organ_bts.xlsx')
    df_bts.to_excel(path, index=False)


def organ_bts_report():
    engine = create_engine(const.ENGINE_OPEX)
    conn = engine.connect()

    query_outputs = text(
        f"select o.*, c.isocalendar_year, c.week, c.weekday from organ_outputs o left join calendar c on o.date = c.date")
    query_bts = text("select * from organ_bts")

    result_outputs = conn.execute(query_outputs)
    df_outputs = pd.DataFrame(result_outputs.all())
    df_outputs = df_outputs.groupby(['organ_id', 'wo_id', 'isocalendar_year', 'week'])['outputs'].sum().reset_index(drop=False)

    result_bts = conn.execute(query_bts)
    df_bts = pd.DataFrame(result_bts.all())

    df_bts_report = pd.merge(df_bts, df_outputs, on=['organ_id', 'wo_id', 'isocalendar_year', 'week'], how='left')
    # bts的公式中，产量大于计划数量时，产量按照计划数量计算
    df_bts_report['outputs'] = df_bts_report['outputs'].where(df_bts_report['outputs'] <= df_bts_report['planned'], df_bts_report['planned'])
    df_bts_report = df_bts_report.groupby(['date', 'organ_id', 'isocalendar_year', 'week']).agg({
        'planned': 'sum',
        'outputs': 'sum'
    }).reset_index(drop=False)

    df_bts_report.to_sql('organ_bts_report', con=engine, if_exists='append', index=False)


def organ_final_rework():

    engine = create_engine(const.ENGINE_OPEX)

    query = 'select * from organ_outputs'

    df = pd.read_sql(query, con=engine)

    print(df.head())


def organ_final_rework_report():
    from pandas.tseries.offsets import BusinessDay

    engine = create_engine(const.ENGINE_OPEX)
    conn = engine.connect()
    # query = text(f"select * from :table;")
    # result = conn.execute(query, {
    #     'table': 'organ_outputs'
    # })
    query = text('select * from organ_outputs;')
    result = conn.execute(query)
    df = pd.DataFrame(result.all())

    df_rework = df.copy()
    df_rework['date'] = df_rework['date'] + BusinessDay(n=3)

    df_rework['rework'] = (df_rework['outputs'] * np.random.uniform(0, 0.04, size=len(df_rework))).astype('int')
    df_rework['qualified'] = df_rework['outputs'] - df_rework['rework']
    df_rework['fault_a'] = (df_rework['rework'] * np.random.uniform(0, 0.4)).astype('int')
    df_rework['fault_b'] = (df_rework['rework'] * np.random.uniform(0, 0.2)).astype('int')
    df_rework['fault_c'] = (df_rework['rework'] * np.random.uniform(0, 0.1)).astype('int')

    path = os.path.join(const.DIR_REPORTS, 'organ_final_rework_report.xlsx')
    df_rework.to_excel(path, index=False)


def calendar_data():
    engine = create_engine(const.ENGINE_OPEX)
    conn = engine.connect()

    start_date = dt.date(2023, 1, 1)
    end_date = dt.date(2027, 12, 31)
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')

    # 创建DataFrame
    calendar_df = pd.DataFrame({
        'date': date_range  # 转换为datetime.date类型
    })

    # calendar_df['year'] = calendar_df['date_in_calendar'].apply(lambda x: x.year)
    # calendar_df['month'] = calendar_df['date_in_calendar'].apply(lambda x: x.month)
    # calendar_df['quarter'] = calendar_df['date_in_calendar'].apply(lambda x: (x.month - 1) // 3 + 1)
    # calendar_df['week'] = calendar_df['date_in_calendar'].apply(lambda x: x.isocalendar()[1])  # ISO周数
    # calendar_df['weekday'] = calendar_df['date_in_calendar'].apply(lambda x: x.weekday())  # 0 表示周一

    calendar_df['isocalendar_year'] = calendar_df['date'].dt.isocalendar().year
    calendar_df['year'] = calendar_df['date'].dt.year
    calendar_df['month'] = calendar_df['date'].dt.month
    calendar_df['quarter'] = calendar_df['date'].apply(lambda x: (x.month - 1) // 3 + 1)
    calendar_df['week'] = calendar_df['date'].dt.isocalendar().week
    calendar_df['weekday'] = calendar_df['date'].dt.weekday

    calendar_df.to_sql('calendar', con=engine, if_exists='append', index=False)


if __name__ == '__main__':
    style_owe_report()