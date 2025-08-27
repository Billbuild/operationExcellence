import easy.constants as const
import easy.easyclass as ec

from django.shortcuts import render


# 主页文件
def index(request):
    factory_names = ec.Organization('c001').children_names
    factory_ids = ec.Organization('c001').children_ids


    dic = const.CONFIG_FACTORY

    factory_list = []
    for item in dic.keys():
        if item.startswith('_'):
            continue
        factory_list.append(item)
    # 因为工厂名称 是用字母开头的，比如 01衢州 02龙游,
    factory_list.sort()

    context = {
        'factory_names': factory_names,
        'factory_ids': factory_ids,

        'factory': factory_list,
        # 01衢州 车间
        'workshops_factory_A': dic[factory_list[0]]['workshops'],
        'workshops_factory_B': dic[factory_list[1]]['workshops'],
    }

    return render(request, 'home/index.html', context)
