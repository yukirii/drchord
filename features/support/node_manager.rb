# encoding: utf-8

require 'drb/drb'
require 'colored'
require 'chukan'
require 'yaml'

class NodeManager
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

node_manager = NodeManager.new

World do
  node_manager
end

AfterConfiguration do
  node_manager.start
end
