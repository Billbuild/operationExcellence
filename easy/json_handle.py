import json
from pathlib import Path


def export_dict_to_json(data, json_file_path):
    """
    用途：将字典内容写到json文件
    :param data:
    :param json_file_path:
    :return:
    """
    with open(json_file_path, 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=4)
    print(f'字典已经保存到 {json_file_path}')

    return None


def load_json_as_dic(json_file_path):
    """
    用途：打开json文件，转为字典
    :param json_file_path:
    :return:
    """
    with open(json_file_path, 'r', encoding='utf-8') as json_file:
        dic = json.load(json_file)

    return dic


if __name__ == '__main__':
    pass
