# encoding: utf-8

require 'drb/drb'
require 'chukan'
include Chukan

Before do
  @node = spawn("ruby main.rb")
  @node.stderr_join(/.*\sJoin\snetwork\scomplete.*/)
end

After do
  @node.kill
end

#
# 共通
#
前提 /^: ノードに接続できる$/ do
  @front = DRbObject::new_with_uri("druby://127.0.0.1:3000")
  @front.active?.should == true
end

#
# シナリオ: Key-Value の put
#
もし /^: Key\-value を put する$/ do
  @ret = @front.dhash.put("hoge", "huga")
end

ならば /^: 返り値に true が返される$/ do
  @ret.should == true
end

かつ /^: get した結果が put した Key\-Value と一致する$/ do
  result = @front.dhash.get("hoge")
  result.should == "huga"
end

#
# シナリオ: put 時に引数を渡さない
#
もし /^: put を引数 nil で実行する$/ do
  @ret = @front.dhash.put(nil, nil)
end

ならば /^: 返り値に false が返される$/ do
  @ret.should == false
end
