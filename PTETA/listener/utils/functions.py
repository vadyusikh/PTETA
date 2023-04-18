import json
from concurrent.futures import ThreadPoolExecutor

from tqdm.auto import tqdm


def dict_special_comparator(dict1, dict2, not_compare_keys=['response_datetime']):
    if not isinstance(not_compare_keys, set):
        not_compare_keys = set(not_compare_keys)
    if not (dict1.keys() ^ dict2.keys()).issubset(not_compare_keys):
        return False
    for k in dict1.keys() & dict2.keys() - not_compare_keys:
        if dict1[k] != dict2[k]:
            return False
    return True


def load_single_response(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return file_path.name, json.load(file)
    except Exception as e:
        print(f"Unable to open : {file_path}")
        print(f"Exception is '{e}'")
        return file_path.name, None


def load_all_responses(folder):
    ## TODO: apply after DATETIME_FILENAME_FORMAT convention works
    # f_path_list = sorted(
    #     list(folder.iterdir()),
    #     #         key=lambda p: datetime.strptime(p.name[-24:-5] , DATETIME_FILENAME_FORMAT)
    #     key=lambda p: datetime.strptime(p.name[-29:-5], DATETIME_FILENAME_FORMAT)
    # )

    f_path_list = sorted(list(folder.iterdir()))

    with ThreadPoolExecutor(max_workers=40) as executor:
        return list(
            tqdm(
                executor.map(load_single_response, f_path_list),
                total=len(f_path_list), miniters=int(len(f_path_list) // 10),
                desc=f"Read {str(folder)} to RAM", ascii=True
            )
        )
