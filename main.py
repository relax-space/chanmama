
import json
import os
import random
import time
from asyncio import Semaphore, gather, get_event_loop
from typing import Tuple

import aiofiles
from aiocsv import AsyncWriter
from aiohttp import ClientSession, TCPConnector


async def req(key: str, page: int, size: int, headers: dict, sub_folder_name: str, session: ClientSession) -> Tuple[int, int, int, int]:

    try:
        url = 'https://api-service.chanmama.com/v2/product/search'
        params = {
            "keyword": key,
            "keyword_type": "",
            "page": page,
            "price": "",
            "duration_author_count": "",
            "size": size,
            "filter_coupon": 0,
            "has_live": 0,
            "has_video": 0,
            "tb_max_commission_rate": "",
            "day_pv_count": "",
            "duration_volume": "",
            "big_category": "",
            "first_category": "",
            "second_category": "",
            "platform": "jinritemai",
            "sort": "auto",
            "order_by": "desc",
            "day_type": 1,
            "most_volume": -1,
            "most_aweme_volume": 0,
            "most_live_volume": 0
        }
        async with session.post(url, json=params, headers=headers) as resp:
            async with aiofiles.open(os.path.join(sub_folder_name, f'{key}.csv'), mode='a', encoding='utf-8-sig') as f:
                res_txt = await resp.read()
                res = json.loads(res_txt, strict=False)
                err_code = res['errCode']
                if err_code != 0:
                    return -1, 0, 0, 0
                data = res['data']
                if not data:
                    return -1, 0, 0, 0
                page_info = data['page_info']
                page_total = int(page_info['totalPage'])
                count_total = int(page_info['totalPage'])
                data_list = data['list']
                rows = [
                    [
                        i['image'],
                        i['title'],
                        i['brand'],
                        i['url'],
                        i['market_price'],

                        i['cat'],
                        i['shop_name'],
                        i['sales'],
                        i['tb_max_commission_rate'],
                        i['day_pv_count'],

                        i['conversion_rate'],
                        i['day_order_count'],
                        i['tb_coupon_price'],

                    ] for i in data_list]
                await AsyncWriter(f).writerows(rows)
                return page, len(data_list), page_total, count_total
    except Exception as e:
        print(e)
        return -1, 0, 0, 0


async def trunk(sem: Semaphore, key: str, page: int, size: int, headers: dict, sub_folder_name: str, session: ClientSession) -> Tuple[int, int, int, int]:
    async with sem:
        return await req(key, page, size, headers, sub_folder_name, session)


async def retry_asyc(sem: Semaphore, key: str, size: int, headers: dict, sub_folder_name: str, page_list: list, page_total, page_count) -> Tuple[int, int]:
    return await common_asyc(sem, key,  size, headers, sub_folder_name, page_list, page_total, page_count)


async def main_asyc(sem: Semaphore, key: str, size: int, headers: dict, sub_folder_name: str) -> Tuple[int, int]:
    async with aiofiles.open(os.path.join(sub_folder_name, f'{key}.csv'), mode='w', encoding='utf-8-sig') as f:
        await AsyncWriter(f).writerow(['image',
                                       'title',
                                       'brand',
                                       'url',
                                       'market_price',

                                       'cat',
                                       'shop_name',
                                       'sales',
                                       'tb_max_commission_rate',
                                       'day_pv_count',

                                       'conversion_rate',
                                       'day_order_count',
                                       'tb_coupon_price'])
    async with ClientSession(connector=TCPConnector(limit=40)) as session:
        page, page_num, page_total, page_count = await trunk(sem, key, 1, size, headers, sub_folder_name, session)
        if page < 0:
            print(f'请求失败 {key}')
            return {-1}, {-1}, 0, 0
        if page_total <= 1:
            return {1}, {1}, page_total, page_count
        page_list = range(2, page_total)
        # ::测试的时候用
        # page_list = page_list[2:3]
        exp_set, act_set = await common_asyc(sem, key,  size, headers, sub_folder_name, page_list, page_total, page_count)
        return exp_set, act_set, page_total, page_count


async def common_asyc(sem: Semaphore, key: str, size: int, headers: dict, sub_folder_name: str,
                      page_list: list, page_total: int, page_count: int) -> Tuple[int, int]:
    t1 = time.time()
    async with ClientSession(connector=TCPConnector(limit=40)) as session:
        page_total += 1
        tasks = []
        exp_set = set()
        for i in page_list:
            exp_set.add(i)
            tasks.append(trunk(sem, key, i, size, headers,
                         sub_folder_name, session))
        act_group_list = await gather(*tasks)
        act_list = []
        total = 0
        for i in act_group_list:
            act_list.append(i[0])
            total += i[1]

        act_set = set(act_list)
        if exp_set != act_set:
            diff = exp_set-act_set
            print(f'下载失败 {round(time.time()-t1)}s {key} 这几个需要重新下载 {diff}')
        else:
            if page_count != total:
                print(f'友情警告 服务响应结果有误 期待{page_count} 实际{total}')
            print(f'下载成功 {round(time.time()-t1)}s {key}')

        return exp_set, act_set


def get_ua():
    contents = read_json('ua_fake.json')
    return random.choice(contents)


def write_json(content: str, file_path: str):
    with open(file_path, mode='w', encoding='utf8') as f:
        json.dump(content, f)


def read_json(file_path: str):
    with open(file_path, mode='r', encoding='utf8') as f:
        return json.load(f)


def main():
    t1 = time.time()
    folder_name = 'data'
    if not os.path.isdir(folder_name):
        os.makedirs(folder_name)

    size = 200

    headers = {
        ':authority: ': 'api-service.chanmama.com',
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json;charset=UTF-8',
        'Host': 'api-service.chanmama.com',
        'Origin': 'https://www.chanmama.com',
        'user-agent': get_ua(),
        'cookie': os.getenv('chanmama_cookie')
    }

    keys = ["女装", "男装", "美妆护理", "鞋包饰品", "日用百货", "母婴玩具", "食品生鲜",
            "运动户外", "鲜花家纺", "宠物农资", "汽车配件", "手机数码", "生活家电", "家装建材", "其他", ]
    sem = Semaphore(1)
    err_file_name = 'err.json'
    for i in keys:
        sub_folder_name = os.path.join(folder_name, i)
        if not os.path.isdir(sub_folder_name):
            os.makedirs(sub_folder_name)
        headers.update(
            {'referer': 'https://www.chanmama.com/promotionRank?big_category=&first_category=&second_category=&keyword={i}'})
        exp_set, act_set, page_total, page_count = get_event_loop().run_until_complete(
            main_asyc(sem, i, size, headers, sub_folder_name))
        # 第一次请求就失败了
        if exp_set == {-1}:
            continue
        write_json({'diff': list(exp_set-act_set),
                    'page_total': page_total, 'page_count': page_count}, os.path.join(sub_folder_name, err_file_name))
        print(f'下载完成 {i}')

    # 重试

    err_list = []
    for i in keys:
        err_path = os.path.join(sub_folder_name, err_file_name)
        if not os.path.isfile(err_path):
            continue
        errs = read_json(err_path)
        exp_set, act_set = get_event_loop().run_until_complete(retry_asyc(
            sem, i,  size, headers, sub_folder_name, errs['diff'], errs['page_total'], errs['page_count']))
        if exp_set != act_set:
            err_list.append(i)
            write_json({'diff': list(exp_set-act_set),
                        'page_total':  errs['page_total'], 'page_count': errs['page_count']}, os.path.join(sub_folder_name, err_file_name))
            print(f'重试失败 {i}')
            continue
        print(f'重试成功 {i} ')
    if err_list:
        print(f'失败列表 {round(time.time() - t1)}s {err_list}')
        return
    print(f'所有成功 {round(time.time() - t1)}s')
    pass


if __name__ == '__main__':
    main()
