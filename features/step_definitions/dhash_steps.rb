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

When /^: ノードに接続できる$/ do
  @front = DRbObject::new_with_uri("druby://127.0.0.1:3000")
  @front.active?.should == true
end

When /^: Key\-value を put する$/ do
  @ret = @front.dhash.put("hoge", "huga")
end

When /^: Value を get する$/ do
  @ret = @front.dhash.get("hoge")
end

When /^: get した結果が put した Key\-Value と一致する$/ do
  result = @front.dhash.get("hoge")
  result.should == "huga"
end

When /^: put を引数 nil で実行する$/ do
  @ret = @front.dhash.put(nil, nil)
end

When /^: 戻り値に true が返される$/ do
  @ret.should == true
end

When /^: 戻り値に false が返される$/ do
  @ret.should == false
end

When /^: 戻り値が nil, false でない$/ do
  @ret.should_not == nil
  @ret.should_not == false
end
