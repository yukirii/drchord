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

require File.expand_path(File.join(drchord_dir, '/lib/front.rb'))
require File.expand_path(File.join(drchord_dir, '/lib/util.rb'))
require 'logger'

front = DRChord::Front.new(Logger.new(STDERR))
front.start

Thread.new do
  puts "Press the enter key to print node info..."
  loop do
    if gets == "\n"
      DRChord::Util.print_node_info(front.chord, front.dhash)
    end
  end
end

front.dhash.start(front.options[:bootstrap_node])
