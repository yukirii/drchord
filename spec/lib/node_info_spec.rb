# encoding: utf-8

require './lib/node_info.rb'
require 'spec_helper'
require 'zlib'

describe DRChord::NodeInformation do
  before do
    @ip = '127.0.0.1'
    @port = 3000
    @info = DRChord::NodeInformation.new(@ip, @port)
  end

  it "正しい id が返される" do
    expect(@info.id).to eq(Zlib.crc32("#{@ip}:#{@port}"))
  end

  it "正しい uri が返される" do
    expect(@info.uri).to eq("druby://#{@ip}:#{@port}?chord")
  end

  it "オプション付き uri が返される" do
    expect(@info.uri("chord")).to eq("druby://#{@ip}:#{@port}?chord")
    expect(@info.uri("dhash")).to eq("druby://#{@ip}:#{@port}?dhash")
  end

  it "存在しないオプションで nil が返される" do
    expect(@info.uri("hogehoge")).to eq(nil)
  end
end

