import marimo

__generated_with = "0.25.2"
app = marimo.App()


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # お勉強

    Bag of Words (BoW) モデルを使ってテキストデータを数値ベクトルに変換する方法と、コサイン類似度を計測する方法を整理してみました。
    慣れるまで略さない方が良い気がするので、長いけど Bag of Words って表現しています。
    コードでも意図的に省略しないようにしているので、冗長だと感じることがあると思います。

    ## Bag of Words とは？

    Bag of Words モデルは、テキストデータを数値ベクトルに変換するための手法の一つみたいです。
    各文書を単語の出現頻度で表現し、文書内の単語の順序や文法構造は無視するっぽいです。

    ## コサイン類似度とは？

    コサイン類似度は、二つのベクトルの間の角度を基に類似度を計算する指標で、特にテキストデータの類似度を計算するのに有効らしいです。
    コサイン類似度は -1 から 1 までの値を取り、1 に近いほど類似度が高いことを示します。

    ## 1. セットアップ

    必要なライブラリをインポートします。
    """)
    return


@app.cell
def _():
    import polars as pl
    from sklearn.feature_extraction.text import CountVectorizer
    from sklearn.metrics.pairwise import cosine_similarity

    return CountVectorizer, cosine_similarity, pl


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 2. データの準備

    サンプルデータを用意します。
    自分の好きなように適当にいじってみるとよりわかりやすくなるかもです。
    """)
    return


@app.cell
def _():
    sample_log1 = "This is the first log"
    sample_log2 = "This is the second log"
    sample_log3 = "Pen pineapple apple pen"

    sample_logs = [sample_log1, sample_log2, sample_log3]
    return (sample_logs,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 3. Bag of Words モデルの作成

    CountVectorizer を使って、ログデータを Bag of Words ベクトルに変換します。
    """)
    return


@app.cell
def _(CountVectorizer, sample_logs):
    count_vectorizer = CountVectorizer()
    bag_of_words_matrix = count_vectorizer.fit_transform(sample_logs)
    return bag_of_words_matrix, count_vectorizer


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 4. 特徴単語の表示

    BoW モデルによって抽出された特徴単語を表示します。
    """)
    return


@app.cell
def _(count_vectorizer):
    feature_names = count_vectorizer.get_feature_names_out()
    feature_names
    return (feature_names,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 5. BoW ベクトルの表示

    BoW 行列をデータフレームに変換し、各単語の出現回数を表示してみます。
    データフレームへの変換は見やすくする目的しかないです。
    """)
    return


@app.cell
def _(bag_of_words_matrix, feature_names, pl):
    bag_of_words_data_frame = pl.DataFrame(
        bag_of_words_matrix.toarray(),
        schema=list(feature_names),
        orient="row",
    )
    bag_of_words_data_frame
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 6. コサイン類似度の計算

    各データ間のコサイン類似度を計算し、類似度行列を表示します。
    結果からもわかるように、データの 1, 2 はめっちゃ似ていますが、3 番目のデータは明後日の方向を向いた内容なので類似度が低いですね。
    """)
    return


@app.cell
def _(bag_of_words_matrix, cosine_similarity, pl):
    similarities = cosine_similarity(bag_of_words_matrix)

    similarity_data_frame = pl.DataFrame(
        similarities,
        schema=["log1", "log2", "log3"],
        orient="row",
    )
    similarity_data_frame
    return


if __name__ == "__main__":
    app.run()
