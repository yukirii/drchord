# encoding: utf-8
require 'spec_helper'
require './lib/node.rb'
require 'zlib'

describe DRChord::Node do
  before :all do
    @options = {:ip => '127.0.0.1', :port => 3000}
    @node = DRChord::Node.new(@options)
  end

  it "node の情報が正しく設定されている" do
    expect(@node.ip).to eq(@options[:ip])
    expect(@node.port).to eq(@options[:port])
  end

  it "id method で node の id が正しく返される" do
    id = Zlib.crc32("#{@options[:ip]}:#{@options[:port]}")
    expect(@node.id).not_to be_nil
    expect(@node.id).to eq(id)
  end

  it "successor が設定できる" do
    succ_info = {:ip => '127.0.0.1', :port => 4000, :id => Zlib.crc32("127.0.0.1:4000")}
    @node.successor = succ_info
    expect(@node.successor).to eq(succ_info)
  end
end
