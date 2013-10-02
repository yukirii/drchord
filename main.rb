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
  opt.on('-e --bootstrap_node', 'bootstrap node (existing node)  IP_ADDR:PORT') {|v| options[:bootstrap_node] = "druby://#{v}?chord" }
  opt.on_tail('-h', '--help', 'show this message') {|v| puts opt; exit }
  begin
    opt.parse!
  rescue OptionParser::InvalidOption
    puts "Error: Invalid option. \n#{opt}"; exit
  end
end

require File.expand_path(File.join(drchord_dir, '/lib/front.rb'))
require File.expand_path(File.join(drchord_dir, '/lib/util.rb'))
require 'logger'

logger = Logger.new(STDERR)
front = DRChord::Front.new(options, logger)

uri = "druby://#{options[:ip]}:#{options[:port]}"
begin
  DRb.start_service(uri, front, :safe_level => 1)
  logger.info "dRuby server start - #{DRb.uri}"
rescue Errno::EADDRINUSE
  logger.error "Error: Address and port already in use. - #{uri}"; exit
end

Thread.new do
  puts "Press the enter key to print node info..."
  loop do
    if gets == "\n"
      DRChord::Util.print_node_info(front.chord)
    end
  end
end

front.dhash.start(options[:bootstrap_node])
