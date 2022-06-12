import functools
import argparse
import pathlib
import subprocess

import pandas as pd
from ja_sentence_segmenter.common.pipeline import make_pipeline
from ja_sentence_segmenter.concatenate.simple_concatenator import concatenate_matching
from ja_sentence_segmenter.normalize.neologd_normalizer import normalize
from ja_sentence_segmenter.split.simple_splitter import split_newline, split_punctuation


# ToDo: 記号が半角に直されてしまうのを修正
# ToDo: ダッシュ記号2つが1つにされてしまうのを修正


def main(input: str) -> None:
    # 以下3行ではMITライセンスで配布された https://github.com/wwwcojp/ja_sentence_segmenter のコードを利用しています。
    split_punc2 = functools.partial(split_punctuation, punctuations=r"。!?")
    concat_tail_te = functools.partial(concatenate_matching, former_matching_rule=r"^(?P<result>.+)(て)$", remove_former_matched=False)
    segmenter = make_pipeline(normalize, split_newline, concat_tail_te, split_punc2)

    p_input = pathlib.Path(input)
    assert p_input.suffix == ".csv", "Input file must be CSV"
    df = pd.read_csv(str(p_input)).rename(columns={"Unnamed: 0": "id_review"})
    df.insert(1, "id_sentence", 0)
    for id_review in range(len(df)):
        df_sub = df[id_review:id_review + 1]
        sentences = list(segmenter(df_sub["review"].iat[0]))
        df_sentences = pd.DataFrame(
            [[id_review, id_sentence, s] for id_sentence, s in enumerate(sentences, 1)],
            columns=["id_review", "id_sentence", "sentences"],
            index=[f"{id_review}-{id_sentence}" for id_sentence in range(1, len(sentences) + 1)]
        )
        df_sub = df_sub.append(df_sentences)
        if id_review == 0:
            df_new = df_sub
        else:
            df_new = pd.concat([df_new, df_sub])

    p_output = p_input.parent / (p_input.stem + "_sentencized_v2.csv")
    df_new.to_csv(str(p_output))
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="Input file path (.csv)")
    args = parser.parse_args()
    main(input=args.input)
