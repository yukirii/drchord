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
options = {:node => "druby://127.0.0.1:3000"}
OptionParser.new do |opt|
  opt.banner = "Usage: ruby #{File.basename($0)} [options]"
  opt.on('-n --node', 'IP_ADDR:PORT') {|v| options[:node] = "druby://#{v}" }
  opt.on_tail('-h', '--help', 'show this message') {|v| puts opt; exit }
  begin
    opt.parse!
  rescue OptionParser::InvalidOption
    puts "Error: Invalid option. \n#{opt}"; exit
  end
end

require File.expand_path(File.join(drchord_dir, '/lib/shell/shell.rb'))
require File.expand_path(File.join(drchord_dir, '/lib/util.rb'))
require 'drb/drb'

node = DRbObject::new_with_uri(options[:node])
Thread.new do
  begin
    loop do
      node.active?
      sleep 5
    end
  rescue DRb::DRbConnError
    puts "Error: Connection failed - #{node.__drburi}"; exit
  end
end

begin
  puts "#{options[:node]} is active? ... #{node.active?}"
  exit unless node.active?

  loop do
    print "\n> "

    input = STDIN.gets.split
    cmd = input.shift
    args = input
    case cmd
    when "status"
      DRChord::Util.print_node_info(node)
    when "get"
      value = node.get(args[0])
      if value == nil || value == false
        puts "Error: Key not found. (#{args[0]})"
      else
        puts "Value: #{value}"
      end
    when "put"
      ret = node.put(args[0], args[1])
      puts ret
    when "delete", "remove"
      ret = node.delete(args[0])
      if ret != nil
        puts "Value #{ret} deleted."
      else
        puts "Error: Key not found. (#{args[0]})"
      end
    when "exit"
      puts "Closing connection..."
      exit
    when "help", "h", "?"
      puts <<"EOS"
Command list:
  status
  put
  get
  delete
  help
  exit
EOS
    else
      puts "Error: No such command - #{cmd}" if cmd.nil? == false && cmd.length > 0
    end
  end
rescue DRb::DRbConnError
  puts "Error: Connection failed - #{node.__drburi}"; exit
rescue Interrupt
  puts "Closing connection..."
end
