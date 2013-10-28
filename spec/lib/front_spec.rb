# encoding: utf-8

require './lib/front.rb'
require './lib/chord.rb'
require 'spec_helper'
require 'drb/drb'

describe DRChord::Front do
  before do
    @default_options = {:ip => '127.0.0.1', :port => 3000}
    @front = DRChord::Front.new
  end

  it "druby uri が返される" do
    expect(@front.uri).to eq("druby://#{@default_options[:ip]}:#{@default_options[:port]}")
  end

  context 'オプション付き URI 指定時' do
    it "Chord オブジェクトが返される" do
      chord = DRbObject::new_with_uri(@front.uri + "?chord")
    end

    it "DHash オブジェクトが返される" do
      dhash = DRbObject::new_with_uri(@front.uri + "?dhash")
    end
  end
end
