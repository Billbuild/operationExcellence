from django import template
import pandas as pd
import numpy as np


register = template.Library()


@register.filter
def percentage(value, decimals=2):
    try:
        return f"{float(value) * 100:.{decimals}f}%"
    except (ValueError, TypeError):
        return "--"


@register.filter
def decimal(value, points=2):
    if value is None:
        return '--'
    # if np.isnan(value):
    #     return '--'
    try:
        return f"{float(value):.{points}f}"
    except(ValueError, TypeError):
        return ""


# @register.filter
# def get_item(dictionary, key):
#     return dictionary.get(key)

@register.filter
def currency(value, decimal):
    from decimal import Decimal

    try:
        num = Decimal(str(value))
        formatted = "{:,.{prec}f}".format(num, prec=decimal)

        # 处理小数点后全为0的情况(可选)
        if decimal > 0 and formatted.endswith('.' + '0' * decimal):
            formatted = formatted[:-decimal - 1]  # 去掉小数点及后面的0

        return formatted
    except ValueError:
        return value



