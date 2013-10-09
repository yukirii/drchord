#!/usr/bin/env ruby
# encoding: utf-8

module DRChord
  class Util
    def self.print_node_info(node)
      puts "successor:   id: #{node.successor.id}\turi: #{node.successor.uri}"
      if node.predecessor.nil?
        puts "predecessor: nil"
      else
        puts "predecessor: id: #{node.predecessor.id}\turi: #{node.predecessor.uri}"
      end

      puts "finger_table: "
      node.finger.each_with_index do |node, i|
        puts "\t#{"%02d" % i}: id: #{node.id}\turi: #{node.uri}"
      end

      puts "successor_list:"
      node.successor_list.each_with_index do |node, i|
        puts "\t#{"%02d" % i}: id: #{node.id}\turi: #{node.uri}"
      end

      puts "key & value:"
      puts "\t#{node.hash_table}"
      puts "replicas:"
      puts "\t#{node.replicas}"
    end
  end
end
