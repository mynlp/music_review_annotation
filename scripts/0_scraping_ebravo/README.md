# scraping

- 『ぶらあぼ電子版』の新着CDレビュー記事をクローリングするためのスクリプトです。
- 最新記事も含めすべての記事をクロールする場合はMongoDBを動作させた上でこのスクリプトを実行する必要がありますが、このリポジトリで配布しているアノテーションデータに対応する記事のデータのみが必要な場合、`ID_to_URL_table.csv`に記載のURLのみへのアクセスによっても必要なデータを入手できます。
- クローリングとスクレイピングは `poetry run python crawl_ebravo.py` で実行でき、結果は MongoDB に保存されると同時に、実行ディレクトリにおいて CSV でも保存されます。
- 本プログラムの利用により生じたいかなる不利益についても作成者は責任を負いません。
