# encoding: utf-8

require 'drb/drb'
require 'colored'
require 'chukan'
require 'yaml'

class MultipleNodes
  include Chukan

  attr_accessor :node_list, :nodes
  def initialize
    node_list_path = File.join(File.expand_path(File.dirname(__FILE__)), 'node_list.yml')
    @node_list = YAML::load_file(node_list_path)[:node_list]
    @nodes = []
  end

  def start
    @node_list.each_with_index do |node, i|
      puts "#{i+1}: Startup node - #{node}".green
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

Given /^: ノードに接続できる$/ do
  uri = "druby://#{node_list.last}"
  @front = DRbObject::new_with_uri(uri)
  @front.active?.should == true
end

Given /^: (\d+) 番目のノードに接続する$/ do |arg1|
  uri = "druby://#{node_list[arg1.to_i]}"
  @front = DRbObject::new_with_uri(uri)
  @front.active?.should == true
end

When /^: Key\-value を put する$/ do
  @value = @key = node_list.first
  @ret = @front.dhash.put(@key, @value)
end

When /^: Value を get する$/ do
  @ret = @front.dhash.get(@key)
end

When /^: get した結果が put した Key\-Value と一致する$/ do
  result = @front.dhash.get(@key)
  result.should == @value
end

When /^: put を引数 nil で実行する$/ do
  @ret = @front.dhash.put(nil, nil)
end

When /^: Key\-Value を delete する$/ do
  @ret = @front.dhash.delete(@key)
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

Given /^: Key\-value を delete する$/ do
  @key = node_list.first
  @ret = @front.dhash.delete(@key)
end

When /^: Key の 担当ノードを再 join する$/ do
  nodes[0] = spawn("ruby main.rb -p #{node_list.first.split(":")[1]} -e #{node_list.last}")
  nodes[0].stderr_join(/.*\sset\ssuccessor.*/)
  sleep 1
end

When /^: stabilize が行われている間でも Value が正しく get できる$/ do
  10.times do
    step ": Value を get する"
    step ": 戻り値が nil, false でない"
    step ": get した結果が put した Key-Value と一致する"
    sleep 1
  end
end
