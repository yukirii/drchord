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

shell = DRChord::DHTShell.new(options)
shell.run
