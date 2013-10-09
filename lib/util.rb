#!/usr/bin/env ruby
# encoding: utf-8

module DRChord
  class Util
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
      puts "replicas:"
      puts "\t#{dhash.replication.replicas}"
    end
  end
end
