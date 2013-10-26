# encoding: utf-8

require './lib/front.rb'
require './lib/chord.rb'
require 'spec_helper'
require 'drb/drb'

describe DRChord::Chord do
  before do
    @front = DRChord::Front.new(nil)
    @chord = @front.chord
    Thread.new do
      @front.start
      @front.dhash.start
    end
  end
end

