from django.urls import path
from apps.measures import views

app_name = 'measures'

urlpatterns = [
    # organ_overview 当月的运营指标概览
    path('organ_overview/<str:factory_id>/', views.organ_overview, name='organ_overview'),
    # organ_card 当日的运营卡片 measures_organ_card.html
    path('organ_card/<str:factory_id>/', views.organ_card, name='organ_card'),
    # organ_table 当日每个车间的运营表格， measures_organ_table.html
    path('organ_table/<str:factory_id>/', views.organ_table, name='organ_table'),
    # organ_chart
    path('organ_chart/<str:factory_id>/', views.organ_chart, name='organ_chart'),
    path('organ_chart/<str:factory_name>/<str:metric_name>/', views.organ_chart, name='organ_chart_with_metric'),
    #
    path('organ_month_report/<str:factory_id>', views.organ_month_report, name='organ_month_report'),

    # 当日的运营日报
    path('organs_daily_report/<str:factory_name>/', views.measures_daily_report, name='measures_daily_report'),
    # 当月的运营月报
    # path('organs_monthly_report/<str:factory_name>/', views.measures_monthly_report, name='measures_monthly_report'),
    # 多角度
    # 目标线
    path('target_line/', views.target_line, name='target_line'),
    # 时间线
    path('timeline/', views.timeline, name='timeline'),
    # 客户线
    path('customer_line/', views.customer_line, name='customer_line'),
    # 设定运营单位的运营目标
    path('metric_setting/', views.metric_setting, name='metric_setting'),
    # 接受 metric_setting.html 的 POST 请求，在 metric_config.json 设置运营目标
    path('metric_config/', views.metric_config, name='metric_config'),
    # 接受 metric_setting.html 的 DELETE 请求，在 metric_config.json 删除运营指标
    path('delete_metric/<str:factory_name>/<str:workshop_name>/<str:metric_name>/', views.delete_metric, name='delete_name'),

    # measures_metric_owe_factory.html
    path('measures_metric_owe/', views.measures_metric_owe_factory, name='measures_metric_owe_factory'),
    # measures_metric_owe_workshop.html
    path('measures_metric_owe_workshop/', views.measures_metric_owe_workshop, name='measures_metric_owe_workshop'),

    # measures_workshop_daily_operation.html
    path('measures_workshop_daily_operation/<str:factory_name>/<str:workshop_name>/', views.measures_workshop_daily_operation, name='measures_workshop_daily_operation'),
    # measures_workshop_charts.html
]
