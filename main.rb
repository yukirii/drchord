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
  opt.banner = "Usage: ruby #{File.basename($0)} [options]"
  opt.on('-i --ip', 'ip address') {|v| options[:ip] = v }
  opt.on('-p --port', 'port') {|v| options[:port] = v.to_i }
  opt.on('-e --bootstrap_node', 'bootstrap node (existing node)  IP_ADDR:PORT') {|v| options[:bootstrap_node] = "druby://#{v}" }
  opt.on_tail('-h', '--help', 'show this message') {|v| puts opt; exit }
  begin
    opt.parse!
  rescue OptionParser::InvalidOption
    puts "Error: Invalid option. \n#{opt}"; exit
  end
end

require File.expand_path(File.join(drchord_dir, '/lib/node.rb'))
require File.expand_path(File.join(drchord_dir, '/lib/util.rb'))
require 'logger'

logger = Logger.new(STDERR)
node = DRChord::Node.new(options, logger)

begin
  uri = "druby://#{options[:ip]}:#{options[:port]}"
  DRb.start_service(uri, node, :safe_level => 1)
rescue Errno::EADDRINUSE
  logger.error "Error: Address and port already in use. - #{uri}"; exit
end
logger.info "dRuby server start - #{DRb.uri}"

node.join(options[:bootstrap_node])

print "Press the enter key to print node info...\n"
Thread.new do
  loop do
    if gets == "\n"
      DRChord::Util.print_node_info(node)
      print "Press the enter key to print node info...\n"
    end
  end
end

begin
  loop do
    if node.active? == true
      # periodically execute methods
      node.stabilize
      node.fix_fingers
      node.fix_successor_list
      node.fix_predecessor
      node.management_replicas
    end
    sleep 5
  end
rescue Interrupt
  logger.info "closing connection.."
  node.leave
end
