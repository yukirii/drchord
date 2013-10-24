# language: ja

機能: 1ノードの分散ハッシュテーブル
  分散ハッシュテーブルを提供するために
  ノード1台のみの環境で DHash をテストする

  @good
  シナリオ: Key-Value を put する
    前提: ノードに接続できる
    もし: Key-value を put する
    ならば: 戻り値に true が返される
    かつ: get した結果が put した Key-Value と一致する

  シナリオ: Key-Value を get する
    前提: ノードに接続できる
    前提: Key-value を put する
    もし: Value を get する
    ならば: 戻り値が nil, false でない
    かつ: get した結果が put した Key-Value と一致する

  シナリオ: Key-Value を delete する
    前提: ノードに接続できる
    前提: Key-value を put する
    前提: Value を get する
    もし: Key-Value を delete する
    ならば: 戻り値に true が返される
    もし: Value を get する
    ならば: 戻り値に false が返される

  @bad
  シナリオ: put 時に引数を渡さない
    前提: ノードに接続できる
    もし: put を引数 nil で実行する
    ならば: 戻り値に false が返される

