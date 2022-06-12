import time
import datetime
import re
import pprint

import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import pandas as pd


# 実装全体において、以下の書籍を参考とした：
# 加藤耕太『Pythonクローリング＆スクレイピング［増補改訂版］』（技術評論社）
# https://gihyo.jp/book/2019/978-4-297-10738-3
# 上記書籍に添付されたプログラム類はCC0ライセンスの下で配布されており、
# 本ファイルでもその一部を改変して利用している。
# https://gihyo.jp/book/2019/978-4-297-10738-3/support


def main() -> None:
    """
    MongoDBへの接続、ぶらあぼの各エントリのURLの取得、それらのクローリングとスクレイピングおよび保存、
    および終了時点でのデータベースの内容のCSV出力を行うメイン関数。
    """
    client = MongoClient("localhost", 27017)
    collection = client.scraping.ebravo
    collection.create_index("key", unique=True)

    urls: list[str] = get_urls()
    session = requests.Session()
    for url in reversed(urls):  # 一応古い記事から順にアクセス
        key = extract_key(url)
        entry = collection.find_one({"key": key})
        if not entry:  # MongoDBに存在しない場合のみクロール
            time.sleep(1)
            response = session.get(url)
            entry = scrape_detail_page(response)
            collection.insert_one(entry)
        pprint.pprint(entry, sort_dicts=False)

    date = datetime.date.today()
    cursor = collection.find()
    df = pd.DataFrame(list(cursor))
    del df["_id"]
    df.to_csv(f"ebravo_{date}.csv")
    # ToDo: 終了メッセージの挿入
    return


def get_urls() -> list[str]:  # ジェネレータとしておいたほうが良い可能性ある？
    """各エントリのURLを取得する。"""
    root_url: str = "https://ebravo.jp/archives/category/nrs"
    urls: list[str] = []
    page_n: int = 1
    session = requests.Session()
    while True:  # 最後のページまでループ
        url: str = root_url
        if page_n > 1:
            url = f"{root_url}/page/{page_n}"
        time.sleep(1)
        response = session.get(url)
        if response.status_code == 404:
            break  # 最後のページを超えていた場合
        soup = BeautifulSoup(response.text, "html.parser")
        entries = soup.find_all("li", attrs={"class": "c-entries__item"})
        for entry in entries:
            urls.append(entry.find("a").get("href"))
        page_n += 1
    return urls


def extract_key(url: str) -> int:
    """各エントリのURLから末尾の記事IDを抽出する。"""
    m = re.search(r"/([^/]+)$", url)  # 最後のスラッシュ以降を取得
    return int(m.group(1))


def scrape_detail_page(response: requests.Response) -> dict:
    """各エントリのHTMLから、必要な情報をスクレイピングする。"""
    soup = BeautifulSoup(response.text, "html.parser")
    key = extract_key(response.url)
    date = soup.select_one(
        "#body > div.l-container > div.l-contents > div.l-contents__body > div > div.l-contents__inner > main > article > header > div > ul > li.c-meta__item.c-meta__item--published"
        ).get_text().split("\n")[-1].strip()
    paragraphs = soup.find_all("div", class_="c-entry__content p-entry-content")[0].find_all("p")
    paragraphs = [p for p in paragraphs if p.get_text().strip() != ""]  # たまに空の変な段落が挟まっている
    main_contents = paragraphs[0].get_text("\n").strip().replace("\n\n", "\n").split("\n")

    if key == 51429:  # 特例処理：曲名の記載が無い
        paragraphs.insert(2, BeautifulSoup("J.S.バッハ：無伴奏チェロ組曲 全曲", "html.parser"))
    elif key == 46783:  # 特例処理：「お詫びと訂正」を削除
        paragraphs.pop(1)
    elif key == 45427:  # 特例処理：曲名の記載が無い
        paragraphs.insert(2, BeautifulSoup("J.C.バッハとW.A.モーツァルトのクラヴィーア協奏曲", "html.parser"))
    elif key == 43956:  # 特例処理：出典の記載が無い
        main_contents.append("（ぶらあぼ2018年6月号より）")
    elif key == 43827:  # 特例処理：筆者の前で改行されていない
        main_contents = main_contents[0].split("文：") + main_contents[1:]

    entry = {
        "url": response.url,
        "key": key,
        "date": date,
        "review": main_contents[0].strip(),
        "author": main_contents[1].replace("文：", "").strip(),
        "source": main_contents[2].replace("（", "").replace("より）", "").strip(),
        "title": paragraphs[1].get_text("\n").split("\n", 1)[1].replace("\n", ""),
        "pieces": paragraphs[2].get_text("\n").replace("\n\n", "\n"),
        "players": paragraphs[3].get_text("\n").replace("\n\n", "\n"),
        "recording_info": paragraphs[4].get_text("\n").replace("\n\n", "\n")
    }
    if len(paragraphs) > 5:
        print()
        print("!!!!!!!!!!")
        print(f"{response.url}: there are {len(paragraphs)} paragraphs")
        entry["other"] = "\t".join([p.get_text("\n") for p in paragraphs[5:]])
    return entry


if __name__ == "__main__":
    main()
