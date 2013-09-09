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

require File.expand_path(File.join(drchord_dir, '/lib/node.rb'))
require File.expand_path(File.join(drchord_dir, '/lib/shell.rb'))

node = DRChord::Node.new(options)

DRb.start_service("druby://#{options[:ip]}:#{options[:port]}", node, :safe_level => 1)
puts "dRuby server start - #{DRb.uri}"

node.add_observer(DRChord::Shell.new)
node.join(options[:existing_node])

print "Press the enter key to print node info...\n"
Thread.new do
  loop do
    if gets == "\n"
      DRChord::Shell.print_node_info(node)
      print "Press the enter key to print node info...\n"
    end
  end
end

begin
  loop do
    node.stabilize
    node.fix_fingers
    node.fix_predecessor

    sleep 5
  end
rescue Interrupt
  puts "closing connection.."
end
