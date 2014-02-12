

あるclientがどこまでメッセージを受け取ったかの情報をトピックごとに持つ必要
あり。

仮想的なクライアントを用意。


TCP connectionごとじゃなくて、CONNごとgoroutineを用意、DISCONが来るまで
保存。CONNが来たら既存のがあるかどうかを見て、あればそのgoroutineに引き
渡す。

- topicごとのACL
