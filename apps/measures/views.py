import re

import numpy as np

import easy.constants as const
import easy.easyclass as ec
import easy.easyfunc as ef
import easy.json_handle as jh

import datetime as dt
import json
from urllib.parse import unquote

from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger

import pandas as pd
from sqlalchemy import create_engine, text

# *** 2025-6-23 ***


# 网页 measures/measures_metric_setting.html
# 浏览 metrics 清单
# 删除 metric 记录
# 设置 metric，该功能隐藏，通过清单上的 设置 按钮打开或者关闭
def metric_setting(request):
    """
    返回一个表单，需要填写工厂 车间 运营指标 运营目标， 这个表单提交后交个 metric_config.html 处理
    需要的数据包括：
    单个工厂名称 单个车间名称
    8个运营指标，8个运营指标的发生值 8个运营指标的目标值 发生值和目标值之间的判断（是否达标）达标绿色，不达标红色 8个运营指标的主要参与值
    :param request:
    :return:
    """
    local_metric_data = jh.load_json_as_dic(const.CONFIGS_DIR / 'metric_config.json')

    # 读取 organization_config.json, 并解析成 python 字典
    organ_data = jh.load_json_as_dic(const.CONFIG_ORGANIZATION)

    # 组织代码 c001 表示公司
    factory_list = ec.Organization('c001').children_names
    # 工厂的下级单位
    factory_workshops = ec.Organization.hierarchy_dic('factory')

    metrics = const.METRICS.metric_name_list()
    metrics_en = const.METRICS.metric_en_list()

    page_number = request.GET.get('page')
    paginator = Paginator(local_metric_data, 10)
    try:
        items = paginator.page(page_number)
    except PageNotAnInteger:
        items = paginator.page(1)
    except EmptyPage:
        items = paginator.page(paginator.num_pages)

    context = {
        'organ_data': organ_data,
        'factories': factory_list,
        'factory_workshops': factory_workshops,
        # 'factory_workshops': json.dumps(factory_workshops),
        'metrics': metrics,
        'metrics_en': metrics_en,
        'metrics_name_en_combine': zip(metrics, metrics_en),
        'items': items
    }

    context_partial = {
        'items': items
    }

    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return render(request, 'measures/measures_partial_metric_setting.html', context_partial)
    else:
        return render(request, 'measures/measures_metric_setting.html', context)


# 网页 measures/measures_metric_setting.html 上的 metric 设置表单
# 接受 metric 设置表单，在 metric_config.json 文件中修改或者添加 metric
def metric_config(request):
    # 解析 HTML 通过 POST 传输的 JSON 数据
    data = json.loads(request.body)
    print(data.values())

    # data.value() 例如：dict_values(['f004', 'owe', '0.75'])
    if all(data.values()):
        organ_id = data.get('organ_id')
        metric_name = data.get('metric')
        target = data.get('target')

        engine = create_engine(const.ENGINE_OPEX)
        with engine.begin() as conn:
            sql = text(f"update organ_metric set metric_target = :target where organization = :organ_id and metric = :metric_name;")
            result = conn.execute(sql, {
                'organ_id': organ_id,
                'metric_name': metric_name,
                'target': target,
            })
            # result 是 < sqlalchemy.engine.cursor.CursorResult object at 0x00000252B6106040 >

    # 将 organ_metric 表的数据本地化到 metric_config.json
    ef.sql_to_json('organ_metric', const.CONFIG_METRIC)

    return JsonResponse({'status': 'success'})


# *** 2025-6-23 ***


def organ_overview(request, factory_id):

    engine = create_engine(const.ENGINE_OPEX)

    context = {}

    metric_en_list = const.METRICS.metric_en_list()

    # 日期
    current_date = dt.date.today()
    fd = ec.FastDate(current_date)
    year, month = fd.prior_month()
    last_month = f"{str(year)}年{str(month)}月"
    same_day_prior_month = fd.same_day_prior_month()
    fd_same_day = ec.FastDate(same_day_prior_month)
    start_date = fd_same_day.month_first_day()
    end_date = fd_same_day.month_last_day()

    context['metric_en_list'] = metric_en_list
    context['last_month'] = last_month

    factory = ec.Organization(factory_id)

    with engine.connect() as conn:
        organ_head = ec.OrganHead(factory_id)
        organ_attendance = ec.OrganAttendance(factory)

        organ_owe = ec.OrganOwe(factory, conn)
        organ_owe.run_period_values((start_date, end_date))
        organ_sam_cost = ec.OrganSamCost(factory, conn)
        organ_sam_cost.run_period_values((start_date, end_date))
        organ_lead_time = ec.OrganLeadTime(factory, conn)
        organ_lead_time.run_period_values((start_date, end_date))
        organ_rft = ec.OrganRft(factory, conn)
        organ_rft.run_period_values((start_date, end_date))
        organ_bts = ec.OrganBts(factory, conn)
        organ_bts.run_period_values((start_date, end_date))
        organ_final_rework = ec.OrganFinalRework(factory, conn)
        organ_final_rework.run_period_values((start_date, end_date))

        # OWE
        # 全员生产效率
        # owe = availability * performance * ( 1 - indirect_rate ) >> 这是第二种计算方法
        owe = organ_owe.owe
        owe_color = organ_owe.value_color(owe, 'html')
        # 工人数 直接人工数 间接人工数
        headcounts, directs, indirects = organ_head.count_head(conn)
        # 间接比
        indirect_rate = indirects / headcounts
        # 工时
        sah = organ_owe.sah
        working_hours = organ_owe.working_hours
        downtime = 3000
        planned_break = 2000
        break_time = downtime + planned_break
        available_hours = working_hours - break_time
        direct_available_hours = available_hours * (1 - indirect_rate)
        indirect_available_hours = available_hours * indirect_rate
        # 停工时长
        break_time = downtime + planned_break
        scrap = organ_owe.scrap
        outputs = organ_owe.outputs
        # 报废率
        scrap_rate = scrap / outputs
        # 线体利用率
        availability = (working_hours - downtime - planned_break) / working_hours
        # 直接生产效率
        performance = sah / direct_available_hours
        context['owe_en_name'] = organ_owe.metric_en_name
        context['owe'] = owe
        context['owe_color'] = owe_color
        context['headcounts'] = headcounts
        context['directs'] = directs
        context['indirects'] = indirects
        context['indirects_rate'] = indirect_rate
        context['working_hours'] = working_hours
        context['direct_available_hours'] = direct_available_hours
        context['indirect_available_hours'] = indirect_available_hours
        context['downtime'] = downtime
        context['planned_break'] = planned_break
        context['sah'] = sah
        context['scrap_rate'] = scrap_rate
        context['availability'] = availability
        context['performance'] = performance

        # Sam Cost
        sam_cost = organ_sam_cost.sam_cost
        sam_cost_color = organ_sam_cost.value_color(sam_cost, 'html')
        cpm = sam_cost * owe
        sah_m = organ_sam_cost.sah_m
        sah = sah_m / 60
        labor_cost = organ_sam_cost.labor_cost
        maintenance_cost = organ_sam_cost.maintenance_cost
        depreciation = organ_sam_cost.depreciation
        rent = organ_sam_cost.rent
        utility_cost = organ_sam_cost.utility_cost
        admin_cost = organ_sam_cost.admin_cost
        others = organ_sam_cost.others
        manufacturing_cost = organ_sam_cost.manufacturing_cost
        context['sam_cost'] = sam_cost
        context['sam_cost_color'] = sam_cost_color
        context['cpm'] = cpm
        context['sah'] = sah
        context['sah_m'] = sah_m
        context['labor_cost'] = labor_cost
        context['maintenance_cost'] = maintenance_cost
        context['depreciation'] = depreciation
        context['rent'] = rent
        context['utility_cost'] = utility_cost
        context['admin_cost'] = admin_cost
        context['others'] = others
        context['manufacturing_cost'] = manufacturing_cost

        # Lead Time
        lead_time = organ_lead_time.lead_time
        offer_cycle = organ_lead_time.offer_cycle
        po_cycle = organ_lead_time.po_cycle
        delivery_cycle = organ_lead_time.delivery_cycle
        workshop_production_cycle = organ_lead_time.workshop_production_cycle
        line_product_cycle = organ_lead_time.line_product_cycle
        exit_cycle = organ_lead_time.exit_cycle
        context['lead_time'] = lead_time
        context['offer_cycle'] = offer_cycle
        context['po_cycle'] = po_cycle
        context['delivery_cycle'] = delivery_cycle
        context['workshop_production_cycle'] = workshop_production_cycle
        context['line_product_cycle'] = line_product_cycle
        context['exit_cycle'] = exit_cycle

        # BTS
        bts = organ_bts.bts
        context['bts'] = bts

    return render(request, 'measures/measures_organ_overview.html', context)


# organ_card 关于工厂车间当日的 metrics
def organ_card(request, factory_id):

    # 报表日期，可以 2024-3-1 也可以是任何一天
    # WARNING： 未对非工作日进行处理
    yesterday = dt.date(2024, 5, 13)
    # 单位-工厂
    organ = ec.Organization(factory_id)
    factory_name = organ.name
    # 工厂下属的车间
    workshop_ids = organ.children_ids
    workshop_names = organ.children_names

    # 车间数据集
    workshop_dataset = []

    engine = create_engine(const.ENGINE_OPEX)

    with engine.connect() as conn:

        for workshop_id in workshop_ids:
            # 单位 - 车间
            organ_workshop = ec.OrganHead(workshop_id)
            workshop_name = organ_workshop.name
            headcounts, directs, indirects = organ_workshop.count_head(conn)
            # indirects = headcounts - directs

            cxt = {
                'factory': factory_name,
                'workshop': workshop_name,
                'start_date': yesterday,
                'end_date': yesterday,
            }

            # OWE
            # 和 metric 相关，和对应的查询无关
            organ_owe = ec.OrganOwe(organ_workshop, conn)
            organ_owe.run_period_values((yesterday, yesterday))

            cxt.update({
                'owe_name_en': organ_owe.metric_en_name,
                'owe_name_cn': organ_owe.metric_cn_name,
                'owe_target': organ_owe.metric_target
            })

            owe_dic = {
                'sah': organ_owe.sah,
                'working_hours': organ_owe.working_hours,
                'direct': directs,
                'indirect': indirects,
                'owe': organ_owe.owe,
                'owe_color': organ_owe.value_color(organ_owe.owe, 'html')
            }

            cxt.update(owe_dic)

            # RFT
            # 和 metric 相关，和对应的查询无关
            organ_rft = ec.OrganRft(organ_workshop, conn)
            organ_rft.run_period_values((yesterday, yesterday))

            cxt.update({
                'rft_name_en': organ_rft.metric_en_name,
                'rft_name_cn': organ_rft.metric_cn_name,
                'rft_target': organ_rft.metric_target
            })

            rft_dic = {
                'rft_output_volume': organ_rft.outputs,
                'rft_non_defects': organ_rft.qualified,
                'rft_defects': organ_rft.defects,
                'rft_color': organ_rft.value_color(organ_rft.rft, 'html'),
                'rft': organ_rft.rft
            }
            cxt.update(rft_dic)

            # Final Rework
            # 和 metric 相关，和对应的查询无关
            organ_final_rework = ec.OrganFinalRework(organ_workshop, conn)
            organ_final_rework.run_period_values((yesterday, yesterday))
            cxt.update({
                'final_rework_name_en': organ_final_rework.metric_en_name,
                'final_rework_name_cn': organ_final_rework.metric_cn_name,
                'final_rework_target': organ_final_rework.metric_target
            })

            final_rework_dic = {
                'final_rework_inspections': organ_final_rework.outputs,
                'final_rework_qualified': organ_final_rework.qualified,
                'final_rework_defects': organ_final_rework.rework,
                'final_rework_color': organ_final_rework.value_color(organ_final_rework.final_rework, 'html'),
                'final_rework': organ_final_rework.final_rework
            }
            cxt.update(final_rework_dic)

            # BTS
            # 和 metric 相关，和对应的查询无关
            organ_bts = ec.OrganBts(organ_workshop, conn)
            last_monday = ec.FastDate(yesterday).last_monday()
            organ_bts.run_period_values((last_monday, last_monday))

            cxt.update({
                'bts_name_en': organ_bts.metric_en_name,
                'bts_name_cn': organ_bts.metric_cn_name,
                'bts_target': organ_bts.metric_target
            })

            bts_dic = {
                'bts_planned': organ_bts.planned,
                'bts_combined': organ_bts.outputs,
                'bts_color': organ_bts.value_color(organ_bts.bts, 'html'),
                'bts': organ_bts.bts
            }
            cxt.update(bts_dic)

            workshop_dataset.append(cxt)

    context = {
        'workshop_dataset': workshop_dataset,
        'workshops': workshop_names
    }

    return render(request, 'measures/measures_organ_card.html', context)


def organ_table(request, factory_id):
    yesterday = dt.date(2025, 5, 13)
    mtd_start_date = ec.FastDate(yesterday).month_first_day()
    ytd_start_date = ec.FastDate(yesterday).year_first_day()
    print(ytd_start_date)
    last_monday = ec.FastDate(yesterday).last_monday()
    end_date = yesterday

    factory_name = ec.Organization(factory_id).name
    workshop_ids = ec.Organization(factory_id).children_ids
    workshop_names = ec.Organization(factory_id).children_names

    workshop_dataset = []

    engine = create_engine(const.ENGINE_OPEX)

    context = {
        'factory_id': factory_id,
        'factory_name': factory_name,
        'workshop_names': workshop_names,
        'mtd_start_date': mtd_start_date,
        'ytd_start_date': ytd_start_date,
        'end_date': yesterday
    }

    with engine.connect() as conn:

        for workshop_id in workshop_ids:
            organ_workshop = ec.Organization(workshop_id)
            workshop_name = organ_workshop.name

            organ_owe = ec.OrganOwe(organ_workshop, conn)
            organ_rft = ec.OrganRft(organ_workshop, conn)
            organ_final_rework = ec.OrganFinalRework(organ_workshop, conn)
            organ_bts = ec.OrganBts(organ_workshop, conn)

            owe_target = organ_owe.metric_target
            rft_target = organ_rft.metric_target
            final_rework_target = organ_final_rework.metric_target
            bts_target = organ_bts.metric_target
            owe_en_name = organ_owe.metric_en_name
            rft_en_name = organ_rft.metric_en_name
            final_rework_en_name = organ_final_rework.metric_en_name
            bts_en_name = organ_bts.metric_en_name

            cxt = {
                'workshop_id': workshop_id,
                'workshop_name': workshop_name,
                'owe_target': owe_target,
                'rft_target': rft_target,
                'final_rework_target': final_rework_target,
                'bts_target': bts_target,
                'owe_en_name': owe_en_name,
                'rft_en_name': rft_en_name,
                'final_rework_en_name': final_rework_en_name,
                'bts_en_name': bts_en_name
            }

            organ_owe.run_period_values((yesterday, yesterday))
            owe_value = organ_owe.owe
            organ_rft.run_period_values((end_date, end_date))
            rft_value = organ_rft.rft
            organ_final_rework.run_period_values((end_date, end_date))
            final_rework_value = organ_final_rework.final_rework
            organ_bts.run_period_values((last_monday, last_monday))
            bts_value = organ_bts.bts
            # owe_value = organ_owe.period_value(conn, (yesterday, yesterday))
            # rft_value = organ_rft.period_value(conn, (end_date, end_date))
            # final_rework_value = organ_final_rework.period_value(conn, (end_date, end_date))
            # bts_value = organ_bts.period_value(conn, (end_date, end_date))

            cxt.update({
                'owe': owe_value,
                'owe_color': organ_owe.value_color(owe_value, 'html'),
                'rft': rft_value,
                'rft_color': organ_rft.value_color(rft_value, 'html'),
                'final_rework': final_rework_value,
                'final_rework_color': organ_final_rework.value_color(final_rework_value, 'html'),
                'bts': bts_value,
                'bts_color': organ_bts.value_color(bts_value, 'html')
            })

            organ_owe.run_period_values((mtd_start_date, end_date))
            owe_mtd_value = organ_owe.owe
            organ_rft.run_period_values((mtd_start_date, end_date))
            rft_mtd_value = organ_rft.rft
            organ_final_rework.run_period_values((mtd_start_date, end_date))
            final_rework_mtd_value = organ_final_rework.final_rework
            organ_bts.run_period_values((mtd_start_date, end_date))
            bts_mtd_value = organ_bts.bts
            # owe_mtd_value = organ_owe.period_value(conn, (mtd_start_date, end_date))
            # rft_mtd_value = organ_rft.period_value(conn, (mtd_start_date, end_date))
            # final_rework_mtd_value = organ_final_rework.period_value(conn, (mtd_start_date, end_date))
            # bts_mtd_value = organ_bts.period_value(conn, (mtd_start_date, end_date))

            cxt.update({
                'owe_mtd': owe_mtd_value,
                'owe_mtd_color': organ_owe.value_color(owe_mtd_value, 'html'),
                'rft_mtd': rft_mtd_value,
                'rft_mtd_color': organ_rft.value_color(rft_mtd_value, 'html'),
                'final_rework_mtd': final_rework_mtd_value,
                'final_rework_mtd_color': organ_final_rework.value_color(final_rework_value, 'html'),
                'bts_mtd': bts_mtd_value,
                'bts_mtd_color': organ_bts.value_color(bts_mtd_value, 'html')
            })

            organ_owe.run_period_values((ytd_start_date, end_date))
            owe_ytd_value = organ_owe.owe
            organ_rft.run_period_values((ytd_start_date, end_date))
            rft_ytd_value = organ_rft.rft
            organ_final_rework.run_period_values((ytd_start_date, end_date))
            final_rework_ytd_value = organ_final_rework.final_rework
            organ_bts.run_period_values((ytd_start_date, end_date))
            bts_ytd_value = organ_bts.bts

            # owe_ytd_value = organ_owe.period_value(conn, (ytd_start_date, end_date))
            # rft_ytd_value = organ_rft.period_value(conn, (ytd_start_date, end_date))
            # final_rework_ytd_value = organ_final_rework.period_value(conn, (ytd_start_date, end_date))
            # bts_ytd_value = organ_bts.period_value(conn, (ytd_start_date, end_date))

            cxt.update({
                'owe_ytd': owe_ytd_value,
                'owe_ytd_color': organ_owe.value_color(owe_ytd_value, 'html'),
                'rft_ytd': rft_ytd_value,
                'rft_ytd_color': organ_rft.value_color(rft_ytd_value, 'html'),
                'final_rework_ytd': final_rework_ytd_value,
                'final_rework_ytd_color': organ_final_rework.value_color(final_rework_ytd_value, 'html'),
                'bts_ytd': bts_ytd_value,
                'bts_ytd_color': organ_bts.value_color(bts_ytd_value, 'html')
            })

            workshop_dataset.append(cxt)

    context['workshop_dataset'] = workshop_dataset

    return render(request, 'measures/measures_organ_table.html', context)


def organ_chart(request, factory_id):
    # def organ_chart(request, factory_name, metric_name='owe'):
    yesterday = dt.date(2025, 5, 13)
    start_date, end_date = ec.FastDate(yesterday).past_days(30), yesterday

    factory = ec.Organization(factory_id)
    factory_name = factory.name
    workshop_names = factory.children_names
    workshop_ids = factory.children_ids

    context = {
        'factory_name': factory_name,
        'workshop_names': workshop_names,
        'metric_names': const.METRICS.metric_en_list(),
        'start_date': start_date,
        'end_date': end_date,
    }

    engine = create_engine(const.ENGINE_OPEX)
    with engine.connect() as conn:
        workshop_dataset = []
        for workshop_id in workshop_ids:
            cxt = {}
            workshop = ec.Organization(workshop_id)
            workshop_name = workshop.name

            for metric_name in ['owe', 'bts', 'rft', 'final_rework']:
                if metric_name == 'owe':
                    metric_cls = ec.OrganOwe
                    min = 0
                    max = 0
                if metric_name == 'bts':
                    metric_cls = ec.OrganBts
                    min = 0
                    max = 0
                if metric_name == 'rft':
                    metric_cls = ec.OrganRft
                    min = 0
                    max = 0
                if metric_name == 'final_rework':
                    metric_cls = ec.OrganFinalRework
                    min = 0
                    max = 0
                metric = metric_cls(workshop, conn)

                metric_name = metric.metric_name
                metric_en_name = metric.metric_en_name
                metric_target = metric.metric_target
                high_better = metric.high_better()
                dates, values = metric.period_date_value(conn, (start_date, end_date))
                targets = [metric_target] * len(dates)

                cxt[metric_name] = {
                    'workshop_name': workshop_name,
                    'metric_name': metric_name,
                    'metric_en_name': metric_en_name,
                    'metric_target': metric_target,
                    'high_better': high_better,
                    'min': min,
                    'max': max,
                    'dates': dates,
                    'values': values,
                    'targets': targets
                }
            workshop_dataset.append(cxt)

    context.update({
        'workshop_dataset': workshop_dataset
    })

    return render(request, 'measures/measures_organ_chart.html', context)


def organ_month_report(request, factory_id):
    # 月报是指指定日期的上一个月的报告
    # 指定日期
    yesterday = dt.date(2025, 5, 13)
    # 自定义日期类， FastDate，建立对象
    fd_yesterday = ec.FastDate(yesterday)
    # 上月同期
    same_day_prior_month = fd_yesterday.same_day_prior_month()
    # 建立 FastDate 对象
    fd_same_day = ec.FastDate(same_day_prior_month)
    # 上个月的第一天
    start_date = fd_same_day.month_first_day()
    # 上个月的最后一天
    end_date = fd_same_day.month_last_day()
    # 当年的第一天
    ytd_start_date = fd_same_day.year_first_day()
    # 年份
    year = same_day_prior_month.year
    # 月份
    month = fd_same_day.month_string()

    # 工厂
    organ_factory = ec.Organization(factory_id)
    # 工厂名称
    factory_name = organ_factory.name

    # metrics = const.METRICS.metric_name_list()
    metrics = ['owe', 'bts', 'rft', 'final_rework']
    # metric_en_list = const.METRICS.metric_en_list()
    metric_en_list = ['OWE', 'BTS', 'RFT', 'Final Rework']
    # 工厂的子单位
    workshops = organ_factory.children_names
    workshop_ids = organ_factory.children_ids

    context = {
        'year': year,
        'month': month,
        'start_date': start_date,
        'end_date': end_date,
        'metric_list': metrics,
        'metric_en_list': metric_en_list,
        'factory_name': factory_name,
        'workshops': workshops
    }

    # overview
    engine = create_engine(const.ENGINE_OPEX)
    with engine.connect() as conn:
        metric_data = {}
        workshop_data = {}
        for metric in metrics:
            if metric == 'owe':
                metric_cls = ec.OrganOwe
            if metric == 'bts':
                metric_cls = ec.OrganBts
            if metric == 'rft':
                metric_cls = ec.OrganRft
            if metric == 'final_rework':
                metric_cls = ec.OrganFinalRework
            metric_factory_obj = metric_cls(organ_factory, conn)

            h_bar_value_list = []
            h_bar_name = f"{metric}_h_bar_list"
            # 折线图 workshop_data 的键名称
            line_date_name = f"{metric}_line_date_list"
            line_value_name = f"{metric}_line_value_list"
            line_color_name = f"{metric}_line_color_list"
            # 折线图 每个值的颜色列表
            line_color_list = []

            metric_factory_obj.run_period_values((start_date, end_date))
            avg_metric_value = getattr(metric_factory_obj, metric_factory_obj.metric_name)
            # avg_metric_value = metric_factory_obj.period_value(conn, (start_date, end_date))
            metric_factory_obj.run_period_values((ytd_start_date, end_date))
            avg_ytd_metric_value = getattr(metric_factory_obj, metric_factory_obj.metric_name)
            # avg_ytd_metric_value = metric_factory_obj.period_value(conn, (ytd_start_date, end_date))
            metric_target = metric_factory_obj.metric_target
            metric_uint = metric_factory_obj.metric_unit
            avg_metric_achieved = metric_factory_obj.is_achievable(avg_metric_value)
            avg_ytd_metric_achieved = metric_factory_obj.is_achievable(avg_ytd_metric_value)
            avg_metric_color = metric_factory_obj.value_color(avg_metric_value, 'js')
            avg_ytd_metric_color = metric_factory_obj.value_color(avg_ytd_metric_value, 'js')

            metric_data.update({
                metric: {
                    'metric_target': metric_target,
                    'avg_metric_achieved': avg_metric_achieved,
                    'avg_metric_value': avg_metric_value,
                    'avg_metric_color': avg_metric_color,
                    'avg_ytd_metric_value': avg_ytd_metric_value,
                    'avg_ytd_metric_achieved': avg_ytd_metric_achieved,
                    'avg_ytd_metric_color': avg_ytd_metric_color,
                    'metric_unit': metric_uint
                }
            })

            for workshop_id in workshop_ids:
                organ_workshop = ec.Organization(workshop_id)
                metric_workshop_obj = metric_cls(organ_workshop, conn)
                metric_workshop_obj.run_period_values((start_date, end_date))
                avg_workshop_metric_value = getattr(metric_factory_obj, metric_factory_obj.metric_name)
                # avg_workshop_metric_value = metric_workshop_obj.period_value(conn, (start_date, end_date))
                avg_workshop_metric_color = metric_workshop_obj.value_color(avg_workshop_metric_value, 'js')

                workshop_name = organ_workshop.name
                workshop_data.setdefault(workshop_name, {})

                h_bar_value_list.append(avg_workshop_metric_value)

                workshop_data[workshop_name].update({
                    metric: {
                        'metric_name': metric,
                        'avg_workshop_metric_value': avg_workshop_metric_value,
                        'avg_workshop_metric_color': avg_workshop_metric_color
                    }
                })

            workshop_data.update({
                h_bar_name: h_bar_value_list
            })

            # 是否应该是 min， 需要依据 high_better 的判断，有待优化
            metric_extreme = min(h_bar_value_list)
            # 找到运营指标表现最差的车间
            workshop_extreme = workshop_ids[h_bar_value_list.index(metric_extreme)]
            organ_workshop_extreme = ec.Organization(workshop_extreme)
            workshop_extreme_name = organ_workshop_extreme.name
            metric_extreme_workshop_obj = metric_cls(organ_workshop_extreme, conn)

            line_zip_date, line_zip_value = metric_extreme_workshop_obj.period_date_value(conn, (start_date, end_date))

            # 折线图的日期列表
            line_dates = list(line_zip_date)
            line_dates = [date.strftime("%m-%d") for date in line_dates]
            workshop_data[line_date_name] = line_dates
            # 折线图的值列表
            line_values = list(line_zip_value)
            line_values = [float(value) for value in line_values]
            workshop_data[line_value_name] = line_values
            # 折线图的颜色列表
            for value in line_values:
                color = metric_extreme_workshop_obj.value_color(value, 'js')
                line_color_list.append(color)
            workshop_data[line_color_name] = line_color_list
            workshop_data['workshop_extreme'] = workshop_extreme_name

    context.update({
        'metric_data': metric_data,
        'workshop_data': workshop_data
    })

    return render(request, 'measures/organ_month_report.html', context)


def measures_daily_report(request, factory_name):
    engine = create_engine(const.ENGINE_OPEX)

    current_date = '2024-03-08'

    # 这里的 workshops 是工厂包含的规定的车间，但在数据集中，车间的数据有可能为空
    # workshops = ec.Factory(factory_name).factory_workshops

    attendance_query = f"select * from organ_attendance_report where factory = '{factory_name}' and date = '{current_date}';"
    df_attendance = pd.read_sql(attendance_query, engine)
    df_attendance['attendance_rate'] = df_attendance['attendances'] / df_attendance['employees']
    attendance_dic = df_attendance.to_dict(orient='records')

    machine_query = f"select * from organ_machine_report where factory = '{factory_name}' and date = '{current_date}';"
    df_machine = pd.read_sql(machine_query, engine)
    machine_dic = df_machine.to_dict(orient='records')

    style_query = f"select * from organ_style_report where factory = '{factory_name}' and date ='{current_date}';"
    df_style = pd.read_sql(style_query, engine)
    style_dic = df_style.to_dict(orient='records')

    transform_dic = {}

    for attendance in attendance_dic:
        workshop = attendance['workshop']
        if workshop not in transform_dic:
            transform_dic[workshop] = {'attendance': None, 'machine': [], 'style': []}
        transform_dic[workshop]['attendance'] = attendance

    for machine in machine_dic:
        workshop = machine['workshop']
        if workshop not in transform_dic:
            transform_dic[workshop] = {'attendance': None, 'machine': [], 'style': []}
        transform_dic[workshop]['machine'].append(machine)

    for style in style_dic:
        workshop = style['workshop']
        if workshop not in transform_dic:
            transform_dic[workshop] = {'attendance': None, 'machine': [], 'style': []}
        transform_dic[workshop]['style'].append(style)

    print(transform_dic)

    workshop_dataset = transform_dic
    workshops = list(workshop_dataset.keys())

    context = {
        'factory': factory_name,
        'workshops': workshops,
        'date': current_date,
        'attendance_dic': attendance_dic,
        'workshop_dataset': transform_dic
    }
    return render(request, 'measures/measures_daily_report.html', context)


def target_line(request):
    engine = create_engine(const.ENGINE_OPEX)
    context = {}

    if request.method == 'GET':
        return render(request, 'measures/measures_target_line.html')
    else:
        organ_id = request.POST.get('organ_id')

        if not re.match(r'^[cfw]\d{3}$', organ_id):
            return JsonResponse({
                'error': '单位编号格式不正确，应为c/f/w+3位数字，例如：w001'
            })

        local_metric_data = jh.load_json_as_dic(const.CONFIGS_DIR / 'metric_config.json')
        # 因为之前有 WIP，后来取消了，所以要在 local_metric_data 中过滤掉 'metric': 'wip'
        local_metric_data = [item for item in local_metric_data if item['metric'] != 'wip']
        # 查询单位的运营目标
        local_metric_data = [item for item in local_metric_data if item['organization'] == organ_id]

        if len(local_metric_data) == 0:
            print(organ_id)
            return JsonResponse({
                'error': '单位编号不存在，请重新输入单位编号。'
            })
        # 需要判断是否是公司 工厂还是车间，如果是车间，需要找到车间的上级单位
        # 例如 {'id': 161, 'organization': 'w016', 'metric': 'owe', 'metric_target': 0.75, 'factory': '苏州工厂', 'workshop': '车间H'}
        for item in local_metric_data:
            item['metric_en_name'] = const.METRICS.metric_en_dic()[item['metric']]
            organ = ec.Organization(item['organization'])
            if organ.type == 'factory':
                item['factory'] = organ.name
                item['workshop'] = '--'
            if organ.type == 'workshop':
                organ_parent = ec.Organization(organ.parent)
                item['factory'] = organ_parent.name
                item['workshop'] = organ.name

            metric = item['metric']
            metric_target = item['metric_target']
            if metric in ['owe', 'bts', 'hot', 'rft', 'final_rework']:
                item['metric_target'] = str(metric_target * 100) + '%'
            if metric == 'sam_cost':
                item['metric_target'] = str(metric_target) + '元'
            if metric == 'lead_time':
                item['metric_target'] = str(metric_target) + '天'

        context['local_metric_data'] = local_metric_data

        return JsonResponse(context)


def timeline(request):

    engine = create_engine(const.ENGINE_OPEX)

    if request.method == 'GET':
        return render(request, 'measures/measures_timeline.html')
    else:
        wo_id = request.POST.get('wo_id')

        if not re.match(r'^wo\d{4}$', wo_id):
            return JsonResponse({
                'error': '工单编号格式不正确，应为wo+4位数字，例如：wo1587'
            })

        context = {}

        with engine.connect() as conn:
            wo_lt = ec.WoLeadTime(wo_id, conn)
            values = wo_lt.wo_values()
            if values == {}:
                return JsonResponse({
                    'error': '工单编号不存在，请重新输入工单编号。'
                })
            organ_id = wo_lt.organ_id
            style_id = wo_lt.style_id
            style = ec.Style(style_id, conn)
            style_name = style.style_name
            input_offer = wo_lt.input_offer
            approve_offer = wo_lt.approve_offer
            input_po = wo_lt.input_po
            supplier_confirm_delivery = wo_lt.supplier_confirm_delivery
            internal_confirm_delivery = wo_lt.internal_confirm_delivery
            approve_po = wo_lt.approve_po
            stock_in_last_bom = wo_lt.stock_in_last_bom
            take_out_first_bom = wo_lt.take_out_first_bom
            product_on_line = wo_lt.product_on_line
            product_off_line = wo_lt.product_off_line
            stock_in_finished = wo_lt.stock_in_finished
            take_out_finished = wo_lt.take_out_finished
            exit_factory_finished = wo_lt.exit_factory_finished
            offer_cycle = wo_lt.offer_cycle
            po_cycle = wo_lt.po_cycle
            delivery_cycle = wo_lt.delivery_cycle
            line_product_cycle = wo_lt.line_product_cycle
            workshop_production_cycle = wo_lt.workshop_production_cycle
            exit_cycle = wo_lt.exit_cycle
            offer_waiting_days = wo_lt.offer_waiting_days
            material_waiting_days = wo_lt.material_waiting_days
            finished_waiting_days = wo_lt.finished_waiting_days

            context['organ_id'] = organ_id
            context['wo_id'] = wo_id
            context['style_id'] = style_id
            context['style_name'] = style_name
            context['input_offer'] = '--' if input_offer == '--' else input_offer.strftime("%Y-%m-%d")
            context['approve_offer'] = '--' if approve_offer == '--' else approve_offer.strftime("%Y-%m-%d")
            context['input_po'] = '--' if input_po == '--' else input_po.strftime("%Y-%m-%d")
            context['supplier_confirm_delivery'] = '--' if supplier_confirm_delivery == '--' else supplier_confirm_delivery.strftime("%Y-%m-%d")
            context['internal_confirm_delivery'] = '--' if internal_confirm_delivery == '--' else internal_confirm_delivery.strftime("%Y-%m-%d")
            context['approve_po'] = '--' if approve_po == '--' else approve_po.strftime("%Y-%m-%d")
            context['stock_in_last_bom'] = '--' if stock_in_last_bom == '--' else stock_in_last_bom.strftime("%Y-%m-%d")
            context['take_out_first_bom'] = '--' if take_out_first_bom == '--' else take_out_first_bom.strftime("%Y-%m-%d")
            context['product_on_line'] = '--' if product_on_line == '--' else product_on_line.strftime("%Y-%m-%d")
            context['product_off_line'] = '--' if product_off_line == '--' else product_off_line.strftime("%Y-%m-%d")
            context['stock_in_finished'] = '--' if stock_in_finished == '--' else stock_in_finished.strftime("%Y-%m-%d")
            context['take_out_finished'] = '--' if take_out_finished == '--' else take_out_finished.strftime("%Y-%m-%d")
            context['exit_factory_finished'] = '--' if exit_factory_finished == '--' else exit_factory_finished.strftime("%Y-%m-%d")
            context['offer_cycle'] = offer_cycle
            context['po_cycle'] = po_cycle
            context['delivery_cycle'] = delivery_cycle
            context['line_product_cycle'] = line_product_cycle
            context['workshop_production_cycle'] = workshop_production_cycle
            context['exit_cycle'] = exit_cycle
            context['offer_waiting_days'] = offer_waiting_days
            context['material_waiting_days'] = material_waiting_days
            context['finished_waiting_days'] = finished_waiting_days

        return JsonResponse(context)


def customer_line(request):
    engine = create_engine(const.ENGINE_OPEX)
    conn = engine.connect()
    context = {}

    if request.method == 'GET':
        return render(request, 'measures/measures_customer_line.html')

    if request.method == 'POST':
        customer_id = request.POST.get('customer_id')
        date_range = request.POST.get('date_range')
        start_date, end_date = tuple(date_range.split(' - '))
        start_date = dt.datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date = dt.datetime.strptime(end_date, '%Y-%m-%d').date()

        if not re.match(r'^cus\d{3}$', customer_id):
            return JsonResponse({
                'error': '客户编号格式不正确，应为cus+3位数字，例如：cus001'
            })

        if end_date < dt.date(2023, 1, 1):
            return JsonResponse({
                'error': '结束时间早于数据库查询内容的起始时间'
            })

        customer = ec.Customer(customer_id, conn)
        customer_owe = ec.CustomerOwe(customer, conn)
        customer_owe.period_values((start_date, end_date))
        customer_sah_value = customer_owe.sah

        if customer_id not in customer.all_ids():
            return JsonResponse({
                'error': '客户编号不存在， 请重新输入客户编号'
            })

        if isinstance(customer_sah_value, str):
            return JsonResponse({
                'error': '未查询到客户在指定期间的产能'
            })

        context['customer_id'] = customer_id
        context['start_date'] = start_date
        context['end_date'] = end_date

        company = ec.Organization('c001')
        company_owe = ec.OrganOwe(company, conn)
        company_owe.run_period_values((start_date, end_date))
        company_sah_value = company_owe.sah

        customer_owe_value = customer_owe.owe
        customer_proportion = customer_sah_value / company_sah_value
        other_proportion = 1 - customer_proportion

        customer_lead_time = ec.CustomerLeadTime(customer, conn)
        customer_lead_time.cycles((start_date, end_date))
        customer_lead_time_value = customer_lead_time.lead_time

        customer_rft = ec.CustomerRft(customer, conn)
        customer_rft.period_values((start_date, end_date))
        customer_rft_value = customer_rft.rft

        customer_final_rework = ec.CustomerFinalRework(customer, conn)
        customer_final_rework.period_values((start_date, end_date))
        customer_final_rework_value = customer_final_rework.final_rework

        context['customer_proportion'] = customer_proportion
        context['other_proportion'] = other_proportion
        context['owe'] = ec.FastValue(customer_owe_value).percentage(2)
        context['lead_time'] = ec.FastValue(customer_lead_time_value).decimals(2)
        context['rft'] = ec.FastValue(customer_rft_value).percentage(2)
        context['final_rework'] = ec.FastValue(customer_final_rework_value).percentage(2)
        context['hot'] = '92.00%'

        return JsonResponse(context)


# 网页 measures/measures_metric_setting.html 上的 metrics 清单（表格）
# 每一行记录有一个删除按钮，点击删除按钮，在metrics 清单和 metric_config.json 文件删除记录
def delete_metric(request, factory_name, workshop_name, metric_name):
    # factory_name = unquote(factory_name)
    # workshop_name = unquote(workshop_name)
    # metric_name = unquote(metric_name)

    # data_from_json = const.CONFIG_METRIC

    json_file_path = const.CONFIGS_DIR / 'metric_config.json'
    data_from_json = jh.load_json_as_dic(json_file_path)
    origin_length = len(data_from_json)
    # 删除符合条件的记录，包形成新的数据集，新的数据集不包含删除的记录
    new_data = [item for item in data_from_json if not(
        item['factory'] == factory_name and item['workshop'] == workshop_name and item['metric'] == metric_name
    )]
    if len(new_data) == origin_length:
        return JsonResponse({'error': f"后台未找到记录${factory_name} ${workshop_name} ${metric_name}"}, status='404')

    jh.export_dict_to_json(new_data, const.CONFIGS_DIR / 'metric_config.json')
    # del data_from_json
    # data_from_json = jh.load_json_as_dic(json_file_path)
    print(data_from_json)

    return JsonResponse({'status': 'success'})


def measures_metric_owe_factory(request):
    start_date = '2024-01-02'
    end_date = '2024-01-02'


    return render(request, 'measures/measures_metric_owe_factory.html')


def measures_metric_owe_workshop(request):
    start_date = '2024-03-02'
    end_date = '2024-03-02'
    # the first day of the month which the end_date belong to
    mtd = pd.to_datetime(end_date).replace(day=1).strftime('%Y-%m-%d')
    # the first day of the year which the end_date belong to
    ytd = pd.to_datetime(end_date).replace(month=1, day=1).strftime('%Y-%m-%d')

    organ_owe = ec.OrganOwe(start_date, end_date)
    df = organ_owe._organ_metric_df()
    # organ_owe_report 包含了工厂的 owe, 要把工厂的 owe 过滤掉，只保留车间的 owe
    df = df[pd.notnull(df['workshop'])]

    # mtd 的 owe
    df_mtd = ec.OrganOwe(mtd, end_date)._organ_metric_df()
    df_mtd_group = df_mtd.groupby(['factory', 'workshop'])[['sah', 'working_hours']].sum().reset_index(drop=False)
    df_mtd_group['mtd'] = df_mtd_group['sah'] / df_mtd_group['working_hours']

    # ytd 的 owe
    df_ytd = ec.OrganOwe(ytd, end_date)._organ_metric_df()
    df_ytd_group = df_ytd.groupby(['factory', 'workshop'])[['sah', 'working_hours']].sum().reset_index(drop=False)
    df_ytd_group['ytd'] = df_ytd_group['sah'] / df_ytd_group['working_hours']

    # 导入 metric_config.json 文件
    df_metric = pd.DataFrame(jh.load_json_as_dic(const.CONFIGS_DIR / 'metric_config.json'))
    # target 的 数据类型是 str，需要转换成 float
    df_metric['target'] = df_metric['target'].astype('float')
    # 只保留 owe 的数据
    df_metric = df_metric[df_metric['metric'] == 'OWE']

    df = pd.merge(df, df_mtd_group, how='left', on=['factory', 'workshop'])
    df = pd.merge(df, df_ytd_group, how='left', on=['factory', 'workshop'])
    df = pd.merge(df, df_metric, how='left', on=['factory', 'workshop'])

    df = df[['factory', 'workshop', 'eff', 'target', 'mtd', 'ytd']]
    df['target'] = df['target'].fillna(0)
    df['color'] = np.where(df['eff'] >= df['target'], 'text-success', 'text-danger')

    # 将 df 转换为 字典，用于后台传递给前端的数据
    owe_dataset = df.to_dict(orient='records')
    # 当点击分页符号时，前端通过 GET 方式传递 page_number
    page_num = request.GET.get('page')
    # 分页，每页限定10条
    paginator = Paginator(owe_dataset, 10)
    try:
        # 如果是通过点击分页符号的方式，传递page_number，那么 items 就是 page_number 页的内容
        items = paginator.page(page_num)
    except PageNotAnInteger:
        # 如果是点击 index.html 页面的链接，这是 paginator.page(page_num) 会报异常 PageNotAnInteger, 此时将 page_number 默认为1，只返回第一页的内容
        items = paginator.page(1)
    except EmptyPage:
        items = paginator.page(paginator.num_pages)

    # 点击 index.html 页面的链接，传递的数据
    context = {
        'start_date': start_date,
        'end_date': end_date,
        # 数据集中删除 workshop 为空的行
        # 'owe_dataset': df.to_dict(orient='records')
        'items': items
    }

    # 点击分页符号时，传递的数据
    context_partial = {
        'items': items
    }

    # 如果是点击分页符号，那么 request.header 'X-Requested-With' == 'XMLHttpRequest'
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return render(request, 'measures/measures_partial_owe_workshop.html', context_partial)
    else:
        return render(request, 'measures/measures_metric_owe_workshop.html', context)


# measures/measures_workshop_daily_operation.html
def measures_workshop_daily_operation(request, factory_name, workshop_name):
    yesterday = dt.date(2024, 3, 25)

    organ_owe = ec.OrganOwe(yesterday, yesterday, factory_name=factory_name, workshop_name=workshop_name)
    owe_actual_owe = organ_owe.actual_owe
    owe_target_owe = organ_owe.metric_target
    owe_text_color = 'text-success' if organ_owe.is_achievable() else 'text-danger'

    context = {
        'daily_operation_day': yesterday,
        'factory': factory_name,
        'workshop': workshop_name,
        # owe
        'owe_actual_owe': owe_actual_owe,
        'owe_target_owe': owe_target_owe,
        'owe_text_color': owe_text_color,
        }
    return render(request, 'measures/measures_workshop_daily_operation.html', context)


if __name__ == '__main__':

    print(any((1, 0)))
    print(all((1, 0)))
    print('-------------')
    print(any((0, 0)))
    print(all((0, 0)))