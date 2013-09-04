#!/usr/bin/env ruby
# encoding: utf-8
require 'optparse'

drchord_dir = File.expand_path(File.dirname(__FILE__))
begin
  ENV['BUNDLE_GEMFILE'] = File.expand_path(File.join(drchord_dir, 'Gemfile'))
  require 'bundler/setup'
rescue LoadError
  puts "Error: 'bundler' not found. Please install it with `gem install bundler`."
  exit
end

config = {:port => 3000, :host => nil}
OptionParser.new do |opt|
  opt.on('-p', '--port', 'port') {|v| config[:port] = v }
  opt.on('-e', '--exists_host', 'exists host IP_ADDR:PORT') {|v| config[:host] = v }
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
p config
