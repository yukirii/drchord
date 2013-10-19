# encoding: utf-8

require './lib/node.rb'
require 'spec_helper'
require 'zlib'
require 'drb/drb'

describe DRChord::Node do
  before do
    @options = {:ip => '127.0.0.1', :port => 3000}
    @node = DRChord::Node.new(@options)
  end
end
