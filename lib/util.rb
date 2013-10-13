#!/usr/bin/env ruby
# encoding: utf-8

drchord_dir = File.expand_path(File.dirname(__FILE__))
require File.expand_path(File.join(drchord_dir, '/node.rb'))
require File.expand_path(File.join(drchord_dir, '/node_info.rb'))
require File.expand_path(File.join(drchord_dir, '/replication_manager.rb'))

module DRChord
  class Util
    M = 32

    def self.print_node_info(chord, dhash)
      puts "successor:   id: #{chord.successor.id}\turi: #{chord.successor.uri}"
      if chord.predecessor.nil?
        puts "predecessor: nil"
      else
        puts "predecessor: id: #{chord.predecessor.id}\turi: #{chord.predecessor.uri}"
      end

      puts "finger_table: "
      chord.finger.each_with_index do |chord, i|
        puts "\t#{"%02d" % i}: id: #{chord.id}\turi: #{chord.uri}"
      end

      puts "successor_list:"
      chord.successor_list.each_with_index do |chord, i|
        puts "\t#{"%02d" % i}: id: #{chord.id}\turi: #{chord.uri}"
      end

      puts "key & value:"
      puts "\t#{dhash.hash_table}"
    end

    def self.between(value, initv, endv)
      return true if initv == endv && initv != value && endv != value
      if initv < endv
        return true if initv < value && value < endv
      else
        return true if value < 0
        return true if ((initv < value && value < 2**M) || (0 <= value && value < endv))
      end
      return false
    end

    def self.Ebetween(value, initv, endv)
      return value == initv ? true : between(value, initv, endv)
    end

    def self.betweenE(value, initv, endv)
      return value == endv ? true : between(value, initv, endv)
    end
  end
end
