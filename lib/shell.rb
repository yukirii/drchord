#!/usr/bin/env ruby
# encoding: utf-8

require 'colored'

module DRChord
  class Shell
    def update(msg)
      puts msg.yellow
    end

    def self.print_node_info(node)
      puts "succ. #{node.successor}"
      puts "pred. #{node.predecessor}"

      puts "finger_table: "
      node.finger.each_with_index do |node, i|
        puts "#{"%02d" % i} : #{node}"
      end

      puts "successor_list:"
      node.successor_list.each_with_index do |node, i|
        puts "#{"%02d" % i} : #{node}"
      end
    end
  end
end
