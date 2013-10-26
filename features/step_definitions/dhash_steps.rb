# encoding: utf-8

require 'drb/drb'
require 'chukan'
require 'yaml'

class MultipleNodes
  include Chukan

  attr_accessor :nodes
  def initialize
    node_list_path = File.join(File.expand_path(File.dirname(__FILE__)), 'node_list.yml')
    @node_list = YAML::load_file(node_list_path)[:node_list]
    @nodes = []
  end

  def start
    @node_list.each_with_index do |node, i|
      cmd = "ruby main.rb -p #{node.split(":")[1]}"
      cmd += " -e #{@node_list[i-1]}" if i != 0
      node = spawn(cmd)
      node.stderr_join(/.*\sJoin\snetwork\scomplete.*/)
      @nodes << node
    end
  end
end

multiple_nodes = MultipleNodes.new

World do
  multiple_nodes
end

AfterConfiguration do
  multiple_nodes.start
end

When /^: ノードに接続できる$/ do
  @front = DRbObject::new_with_uri("druby://127.0.0.1:11000")
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

When /^: Key\-Value を delete する$/ do
  @ret = @front.dhash.delete("hoge")
end

When /^: delete を引数 nil で実行する$/ do
  @ret = @front.dhash.delete(nil)
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

When /^: Key の 担当ノードを Kill する$/ do
  nodes.first.kill
end
