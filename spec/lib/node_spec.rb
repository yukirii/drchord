# encoding: utf-8

require './lib/front.rb'
require './lib/node.rb'
require './lib/node_info.rb'
require 'spec_helper'
require 'logger'
require 'zlib'
require 'drb/drb'

describe DRChord::Node do
  before do
    @options = {:ip => '127.0.0.1', :port => 3000}
    @node = DRChord::Node.new(@options)
  end

  it "node の情報が正しく設定されている" do
    node_info = DRChord::NodeInformation.new(@options[:ip], @options[:port])
    expect(@node.info.ip).to eq(node_info.ip)
    expect(@node.info.port).to eq(node_info.port)
  end

  it "id method で node の id が正しく返される" do
    id = Zlib.crc32("#{@options[:ip]}:#{@options[:port]}")
    expect(@node.id).not_to be_nil
    expect(@node.info.id).to eq(id)
  end

  it "info method で node の情報が正しく取得できる" do
    expect(@node.info).not_to be_nil
    expect(@node.info.id).to eq(Zlib.crc32("#{@options[:ip]}:#{@options[:port]}"))
    expect(@node.info.uri).to eq("druby://#{@options[:ip]}:#{@options[:port]}?chord")
    expect(@node.info.uri("chord")).to eq("druby://#{@options[:ip]}:#{@options[:port]}?chord")
    expect(@node.info.uri("dhash")).to eq("druby://#{@options[:ip]}:#{@options[:port]}?dhash")
  end

  it "successor が設定できる" do
    successor_info = DRChord::NodeInformation.new(@options[:ip], @options[:port])
    @node.successor = successor_info
    expect(@node.successor).to eq(successor_info)
  end
end
