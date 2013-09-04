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

options = {}
OptionParser.new do |opt|
  opt.on('-i --ip', 'ip address') {|v| options[:ip] = v }
  opt.on('-p --port', 'port') {|v| options[:port] = v }
  opt.on('-e --existing_node', 'existing node IP_ADDR:PORT') {|v| options[:host] = v }
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

node = DRChord::Node.new(options)
