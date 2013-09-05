#!/usr/bin/env ruby
# encoding: utf-8

drchord_dir = File.expand_path(File.dirname(__FILE__))
begin
  ENV['BUNDLE_GEMFILE'] = File.expand_path(File.join(drchord_dir, 'Gemfile'))
  require 'bundler/setup'
rescue LoadError
  puts "Error: 'bundler' not found. Please install it with `gem install bundler`."
  exit
end

require 'optparse'
require './lib/node.rb'

options = {:ip => '127.0.0.1', :port => 3000}
OptionParser.new do |opt|
  opt.on('-i --ip', 'ip address') {|v| options[:ip] = v }
  opt.on('-p --port', 'port') {|v| options[:port] = v.to_i }
  opt.on('-e --existing_node', 'existing node IP_ADDR:PORT') {|v| options[:existing_node] = v }
  opt.on_tail('-h', '--help', 'show this message') do |v|
    opt.banner = "Usage: ruby #{File.basename($0)} [options]"
    puts opt
    exit
  end

  begin
    opt.parse!
  rescue OptionParser::InvalidOption
    puts "Invalid option. \n#{opt}"
    exit
  end
end

options
node = DRChord::Node.new(options)
DRb.start_service("druby://#{options[:ip]}:#{options[:port]}", node, :safe_level => 1)
puts "Node: druby server start - #{DRb.uri}"

if options[:existing_node].nil?
  node.join
else
  node.join(options[:existing_node])
end

loop do
  node.stabilize
  node.fix_fingers
  node.fix_successor_list

  puts "succ. #{node.successor[:uri]}  pred. #{node.predecessor[:uri]}"
  puts "finger_table"
  node.finger.each_with_index do |node, i|
    puts "#{"%02d" % i} : #{node}"
  end
  puts "successor_list"
  node.successor_list.each_with_index do |node, i|
    puts "#{"%02d" % i} : #{node}"
  end
  puts
  sleep 10
end
