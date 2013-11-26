# language: ja

機能: 分散ハッシュテーブル
  分散ハッシュテーブルを提供するために
  複数ノードの想定で DHT を構成し
  DHash を利用するクライアントの視点でテストする

  @good
  シナリオ: Key-Value を put する
    前提: 0 番目のノードに接続する
    もし: Key-value を put する
    ならば: 戻り値に true が返される
    かつ: get した結果が put した Key-Value と一致する

  シナリオ: Key-Value を get する
    前提: 0 番目のノードに接続する
    前提: Key-value を put する
    もし: Value を get する
    ならば: 戻り値が nil, false でない
    かつ: get した結果が put した Key-Value と一致する

  シナリオ: Key-Value を delete する
    前提: 0 番目のノードに接続する
    前提: Key-value を put する
    前提: Value を get する
    前提: 戻り値が nil, false でない
    もし: Key-Value を delete する
    ならば: 戻り値に true が返される
    もし: Value を get する
    ならば: 戻り値に false が返される

  @bad
  シナリオ: put 時に引数を渡さない
    前提: 0 番目のノードに接続する
    もし: put を引数 nil で実行する
    ならば: 戻り値に false が返される

  シナリオ: delete 時に引数を渡さない
    前提: 0 番目のノードに接続する
    もし: delete を引数 nil で実行する
    ならば: 戻り値に false が返される

  @churn
  シナリオ: Key の担当ノードを Kill する
     前提: 0 番目のノードに接続する
     前提: Key-value を put する
     前提: Value を get する
     前提: 戻り値が nil, false でない
     前提: 1 番目のノードに接続する
     もし: Key の 担当ノードを Kill する
     かつ: Value を get する
     ならば: 戻り値が nil, false でない
     かつ: get した結果が put した Key-Value と一致する

   シナリオ: 既に保存されている Key の新たな担当ノードが新規加入する
      前提: 4 番目のノードに接続する
      前提: Key-value を delete する
      前提: Key-value を put する
      前提: Value を get する
      前提: 戻り値が nil, false でない
      もし: Key の 担当ノードを再 join する
      かつ: stabilize が行われている間でも Value が正しく get できる
