# encoding: utf-8

require './lib/node.rb'
require 'spec_helper'
require 'zlib'
require 'drb/drb'

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

  it "info method で node の情報が正しく取得できる" do
    expect(@node.info).not_to be_nil
    expect(@node.info[:ip]).to eq(@options[:ip])
    expect(@node.info[:port]).to eq(@options[:port])
    expect(@node.info[:id]).to eq(Zlib.crc32("#{@options[:ip]}:#{@options[:port]}"))
    expect(@node.info[:uri]).to eq("druby://#{@options[:ip]}:#{@options[:port]}")
  end

  it "successor が設定できる" do
    succ_info = {:ip => '127.0.0.1', :port => 4000, :id => Zlib.crc32("127.0.0.1:4000")}
    @node.successor = succ_info
    expect(@node.successor).to eq(succ_info)
  end
end

describe "Create Network" do
  before :all do
    @default_options = {:ip => '127.0.0.1', :port => 3000}
    @node = DRChord::Node.new(@default_options)
    DRb.start_service(@node.info[:uri], @node, :safe_level => 1)
    @node.join
  end

  it "successor がそのノード自身に設定されている" do
    expect(@node.successor).to eq(@node.info)
  end

  it "predecessor が nil である" do
    expect(@node.predecessor).to be_nil
  end

  it "finger table が正しく作成されている" do
    expect(@node.finger.count).to eq(DRChord::Node::M)
    @node.finger.each do |n|
      expect(n).to eq(@node.info)
    end
  end

  it "successor list が正しく作成されている" do
    expect(@node.successor_list.count).to eq(DRChord::Node::SLIST_SIZE)
    @node.successor_list.each do |n|
      expect(n).to eq(@node.info)
    end
  end

  it "active が true である" do
    expect(@node.active?).to be_true
  end

  it "自ノードと同じ ID の lookup で自ノードの情報が返される" do
    id = Zlib.crc32("#{@default_options[:ip]}:#{@default_options[:port]}")
    expect(@node.find_successor(id)).to eq(@node.info)
  end

  describe "stabilize" do
    before :all do
      @node.stabilize
    end

    it "predecessor が自ノードに設定される" do
      expect(@node.predecessor).to eq(@node.info)
    end
  end

  after :all do
    DRb.stop_service
  end
end
